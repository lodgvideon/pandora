package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/afero"
	"github.com/yandex/pandora/core"
	"github.com/yandex/pandora/core/aggregator"
	"github.com/yandex/pandora/core/aggregator/netsample"
	"github.com/yandex/pandora/core/coreutil"
	"go.uber.org/zap"
	"log"
	"strconv"
	"time"
)

type Aggregator struct {
	aggregator.Reporter
	Log             *zap.Logger
	PhoutAggregator netsample.Aggregator
	Config          ClickhouseConfig
	Conn            driver.Conn
	currentSize     uint32
	batch           driver.Batch
}

func NewAggregator(fs afero.Fs, conf Config) (core.Aggregator, error) {
	phout, err := netsample.NewPhout(fs, conf.Phout)
	if err != nil {
		return nil, err
	}
	if !conf.ClickhouseConfig.Enabled {
		return netsample.WrapAggregator(phout), nil
	}
	return NewRawAggregator(conf.ClickhouseConfig, phout), nil
}
func NewRawAggregator(clickhouseConfig ClickhouseConfig, aggr netsample.Aggregator) *Aggregator {

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{clickhouseConfig.Address},
		Auth: clickhouse.Auth{
			Username: clickhouseConfig.Username,
			Password: clickhouseConfig.Password,
		},

		MaxOpenConns: clickhouseConfig.MaxOpenConns,
		Settings: clickhouse.Settings{
			"max_execution_time": clickhouseConfig.MaxExecutionTime,
		},

		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: clickhouseConfig.Debug,
	})
	if clickhouseConfig.IsDefineDDL {
		var CreateDatabaseDdl = "create database IF NOT EXISTS " + clickhouseConfig.Database
		err := conn.Exec(context.Background(), CreateDatabaseDdl)
		if err != nil {
			zap.L().Panic("Unable to create database", zap.Error(err))
		}
		var createTableDdl = "create table IF NOT EXISTS" +
			"    " + clickhouseConfig.Database + ".pandora_raw_results_data" +
			"            (              timestamp_sec DateTime," +
			"               timestamp_millis UInt64," +
			"                profile LowCardinality(String)," +
			"                run_id LowCardinality(String)," +
			"                hostname LowCardinality(String)," +
			"                label LowCardinality(String)," +
			"               cnt UInt64," +
			"                errors UInt64, " +
			"               avg_time Float64, " +
			"               net_code LowCardinality(String), " +
			"               error String                ) engine = MergeTree" +
			" PARTITION BY toYYYYMMDD(timestamp_sec) ORDER BY (timestamp_sec)  SETTINGS index_granularity = 8192"

		err = conn.Exec(context.Background(), createTableDdl)
		if err != nil {
			zap.L().Panic("Unable to create Table pandora_raw_results_data", zap.Error(err))
		}
		var templateStatsDDL = "CREATE MATERIALIZED VIEW IF NOT EXISTS " +
			clickhouseConfig.Database + ".pandora_statistic (\n" +
			"`timestamp_sec` DateTime Codec(DoubleDelta, LZ4),\n" +
			"`timestamp_millis` UInt64 Codec(DoubleDelta, LZ4),\n" +
			"`profile` LowCardinality(String),\n" +
			"`run_id` LowCardinality(String),\n" +
			"`hostname` String Codec(LZ4),\n" +
			"`label` LowCardinality(String),\n" +
			"`cnt` UInt64 Codec(Gorilla, LZ4),\n" +
			"`errors` UInt64 Codec(Gorilla, LZ4),\n" +
			"`avg_time` Float64 Codec(Gorilla, LZ4),\n" +
			"`net_code` LowCardinality(String),\n" +
			"`error` String Codec(LZ4))\n" +
			"    ENGINE = MergeTree\n" +
			"        PARTITION BY toYYYYMM(timestamp_sec)\n" +
			"        ORDER BY (timestamp_sec,\n" +
			"                  profile,\n" +
			"                  run_id,\n" +
			"                  hostname,\n" +
			"                  label,\n" +
			"                  net_code, error)\n" +
			"        SETTINGS index_granularity = 8192\n" +
			" AS\n" +
			"SELECT timestamp_sec,\n" +
			"       timestamp_millis,\n" +
			"       profile,\n" +
			"       run_id,\n" +
			"       hostname,\n" +
			"       label,\n" +
			"       cnt,\n" +
			"       errors,\n" +
			"       avg_time,\n" +
			"       net_code,\n" +
			"       error\n" +
			" FROM " +
			clickhouseConfig.Database + ".pandora_raw_results_data"
		err = conn.Exec(context.Background(), templateStatsDDL)
		if err != nil {
			zap.L().Panic("Unable to create Table MATERIALIZED VIEW Pandora_statistic", zap.Error(err))

		}

		var dbTemplateBuff = "create table IF NOT EXISTS " +
			clickhouseConfig.Database + ".pandora_results\n" +
			"(\n" +
			"\ttimestamp_sec DateTime,\n" +
			"\ttimestamp_millis UInt64,\n" +
			"\tprofile LowCardinality(String),\n" +
			"\trun_id LowCardinality(String),\n" +
			"\thostname LowCardinality(String),\n" +
			"\tlabel LowCardinality(String),\n" +
			"\tcnt UInt64,\n" +
			"\terrors UInt64,\n" +
			"\tavg_time Float64,\n" +
			"\treq String,\n" +
			"\tresp String,\n" +
			"\tnet_code LowCardinality(String),\n" +
			"\terror String\n" +
			")\n" +
			" engine = Buffer(" + clickhouseConfig.Database + ", pandora_raw_results_data, 16, 10, 60, 10000, 100000, 1000000, 10000000)"
		err = conn.Exec(context.Background(), dbTemplateBuff)
		if err != nil {
			zap.L().Panic("Unable to create Table pandora_results", zap.Error(err))
		}
	}

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+clickhouseConfig.Database+".pandora_results")
	if err != nil {
		zap.L().Panic("Unable to create first Batch", zap.Error(err))
	}

	return &Aggregator{
		Config:          clickhouseConfig,
		Conn:            conn,
		PhoutAggregator: aggr,
		currentSize:     0,
		batch:           batch,
		Reporter:        *aggregator.NewReporter(clickhouseConfig.ReporterConfig),
	}
}

func (c *Aggregator) Run(ctx context.Context, deps core.AggregatorDeps) (err error) {
	phoutCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	phoutDone := make(chan error)
	go func() {
		phoutDone <- c.PhoutAggregator.Run(phoutCtx, deps)
	}()
	clickhouseDone := make(chan error)
	go func() { clickhouseDone <- c.run(ctx, deps) }()
	select {
	case clickhouseErr := <-clickhouseDone:
		return clickhouseErr
	case phoutErr := <-phoutDone:
		return phoutErr
	}
}
func (c *Aggregator) getBatch(ctx context.Context) (driver.Batch, error) {
	batch, err := c.Conn.PrepareBatch(ctx, "INSERT INTO "+c.Config.Database+".pandora_results")
	if err != nil {
		log.Print("Error during creating batch", err)
	}
	return batch, err
}

func (c *Aggregator) handleSample(sample core.Sample) (err error) {
	ctx := context.Background()

	if c.batch == nil {
		batch, _ := c.getBatch(ctx)
		c.batch = batch
	}
	var errorCount = 0
	s := sample.(*netsample.Sample)
	c.PhoutAggregator.Report(s)
	if s.Err() != nil {
		errorCount = 1
	}

	tags := s.Tags()
	e := ""
	if s.Err() != nil {
		e = s.Err().Error()

	}
	err = c.batch.Append(
		s.Timestamp(),
		uint64(s.Timestamp().UnixMilli()),
		c.Config.ProfileName,
		c.Config.RunId,
		c.Config.Hostname,
		tags,
		uint64(1),
		uint64(errorCount),
		float64(s.Duration()),
		"",
		"",
		strconv.Itoa(s.ProtoCode()),
		e,
	)
	if err != nil {
		zap.L().Warn("Error during appending to Batch", zap.Error(err))
	}
	c.currentSize++

	if c.currentSize >= c.Config.BatchSize {
		c.TrySendBatch(&c.batch, 0)
		c.batch = nil
		c.currentSize = 0
	}
	coreutil.ReturnSampleIfBorrowed(sample)
	return
}

func (c *Aggregator) TrySendBatch(batch *driver.Batch, recursionDepth int) {
	if recursionDepth < 10 {
		i := *batch
		err := i.Send()
		if err != nil {
			zap.L().Warn("Error during appending to Batch", zap.Error(err))
			c.TrySendBatch(batch, recursionDepth+1)
		}
	} else {
		zap.L().Warn("Send Batch Retries Exceed. Dropping current Batch. Check your clickhouse Settings")
	}

}

func (c *Aggregator) run(ctx context.Context, deps core.AggregatorDeps) (err error) {
HandleLoop:
	for {
		select {
		case sample := <-c.Incoming:
			err = c.handleSample(sample)
			if err != nil {
				deps.Log.Error(err.Error())
				return
			}
		case <-ctx.Done():
			break HandleLoop // Still need to handle all queued samples.
		}
	}
	//Final send after Shoot Finished
	if c.currentSize > 0 {
		err = c.batch.Send()
		if err != nil {
			log.Println("Error during final Batch", err)
		}
	}
	for {
		select {
		case sample := <-c.Incoming:
			err = c.handleSample(sample)
			if err != nil {
				return
			}
		default:
			return nil
		}
	}

}

type Config struct {
	Phout            netsample.PhoutConfig `config:",squash"`
	ClickhouseConfig ClickhouseConfig      `config:"clickhouse_config"`
}

type ClickhouseConfig struct {
	Address          string                    `config:"address" validate:"required"`
	Database         string                    `config:"database" validate:"required"`
	Username         string                    `config:"username" validate:"required"`
	Password         string                    `config:"password" validate:"required"`
	BatchSize        uint32                    `config:"batch_size" validate:"required"`
	MaxOpenConns     int                       `config:"max_connections"`
	MaxIdleConns     uint32                    `config:"max_iddle"`
	MaxExecutionTime uint32                    `config:"max_execution_time"`
	ConnMaxLifetime  time.Duration             `config:"conn_lifetime" validate:"required"`
	ReporterConfig   aggregator.ReporterConfig `config:",squash"`
	ProfileName      string                    `config:"profile" validate:"required"`
	RunId            string                    `config:"run_id" validate:"required"`
	Hostname         string                    `config:"hostname" validate:"required"`
	IsDefineDDL      bool                      `config:"define_ddl"`
	Enabled          bool                      `config:"clickhouse_enabled"`
	Debug            bool                      `config:"debug"`
}

func NewDefaultConfiguration() Config {
	return Config{
		Phout: netsample.DefaultPhoutConfig(),
		ClickhouseConfig: ClickhouseConfig{
			Enabled:  false,
			Address:  "127.0.0.1:9000",
			Database: "pandora_stats",
			Username: "default",
			Password: "default",
			ReporterConfig: aggregator.ReporterConfig{
				SampleQueueSize: 300000,
			},
			BatchSize:        500,
			MaxExecutionTime: 120,
			MaxOpenConns:     10,
			MaxIdleConns:     5,
			Debug:            false,
			ConnMaxLifetime:  time.Hour,
			ProfileName:      "default",
			RunId:            time.Now().String(),
			Hostname:         "localhost",
			IsDefineDDL:      true,
		}}
}
