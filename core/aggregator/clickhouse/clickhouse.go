package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/yandex/pandora/core"
	"github.com/yandex/pandora/core/aggregator"
	"github.com/yandex/pandora/core/aggregator/netsample"
	"github.com/yandex/pandora/core/coreutil"
	"log"
	"strconv"
	"time"
)

type Aggregator struct {
	aggregator.Reporter
	ClickhouseConfig Config
	Conn             driver.Conn
	currentSize      uint32
	batch            driver.Batch
}

func NewRawAggregator(clickhouseConfig Config) *Aggregator {

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{clickhouseConfig.Address},
		Auth: clickhouse.Auth{
			//Database: clickhouseConfig.Database,
			Username: clickhouseConfig.Username,
			Password: clickhouseConfig.Password,
		},
		MaxOpenConns: clickhouseConfig.MaxOpenConns,
		//TLS: &tls.Config{
		//	InsecureSkipVerify: true,
		//},
		Settings: clickhouse.Settings{
			"max_execution_time": 120,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	})
	if clickhouseConfig.IsDefineDDL {
		var CreateDatabaseDdl = "create database IF NOT EXISTS " + clickhouseConfig.Database
		err := conn.Exec(context.Background(), CreateDatabaseDdl)
		if err != nil {
			log.Panic(err)
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
			log.Panic(err)
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
			log.Panic(err)
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
			log.Panic(err)
		}
	}

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+clickhouseConfig.Database+".pandora_results")
	if err != nil {
		log.Panic("Unable to create Clickhouse Connection")

	}

	return &Aggregator{ClickhouseConfig: clickhouseConfig, Conn: conn, batch: batch, Reporter: *aggregator.NewReporter(clickhouseConfig.ReporterConfig)}
}

func (c *Aggregator) Run(ctx context.Context, deps core.AggregatorDeps) (err error) {
HandleLoop:
	for {
		select {
		case sample := <-c.Incoming:
			err := c.handleSample(sample)
			if err != nil {
				log.Panic(err.Error())
				return err
			}
		case <-ctx.Done():
			break HandleLoop // Still need to handle all queued samples.
		}
	}
	//Final send after Shoot Finished
	if c.currentSize > 0 {
		err := c.batch.Send()
		if err != nil {
			log.Println("Error during final Batch", err)
		}
	}
	return
}
func (c *Aggregator) getBatch(ctx context.Context) (driver.Batch, error) {
	batch, err := c.Conn.PrepareBatch(ctx, "INSERT INTO "+c.ClickhouseConfig.Database+".pandora_results")
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
	if s.Err() != nil {
		errorCount = 1
	}

	tags := s.Tags()
	e := ""
	if s.Err() != nil {
		e = s.Err().Error()

	}
	//	"\ttimestamp_sec DateTime,\n" +
	//			"\ttimestamp_millis UInt64,\n" +
	//			"\tprofile LowCardinality(String),\n" +
	//			"\trun_id LowCardinality(String),\n" +
	//			"\thostname LowCardinality(String),\n" +
	//			"\tlabel LowCardinality(String),\n" +
	//			"\tcnt UInt64,\n" +
	//			"\terrors UInt64,\n" +
	//			"\tavg_time Float64,\n" +
	//			"\treq String,\n" +
	//			"\tresp String,\n" +
	//			"\tnet_code LowCardinality(String)\n" +
	err = c.batch.Append(
		s.Timestamp(),
		uint64(s.Timestamp().UnixMilli()),
		c.ClickhouseConfig.ProfileName,
		c.ClickhouseConfig.RunId,
		c.ClickhouseConfig.Hostname,
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
		log.Panic("Error during appending to Batch", err)
	}
	c.currentSize++

	if c.currentSize >= c.ClickhouseConfig.BatchSize {
		err := c.batch.Send()
		if err != nil {
			log.Panic("Error during sending batch")
		}
		c.batch = nil
		c.currentSize = 0
	}

	coreutil.ReturnSampleIfBorrowed(sample)
	return
}

type Config struct {
	Address         string                    `config:"address" validate:"required"`
	Database        string                    `config:"database" validate:"required"`
	Username        string                    `config:"username" validate:"required"`
	Password        string                    `config:"password" validate:"required"`
	BatchSize       uint32                    `config:"batch_size" validate:"required"`
	MaxOpenConns    int                       `config:"max_connections"`
	MaxIdleConns    uint32                    `config:"max_iddle"`
	ConnMaxLifetime time.Duration             `config:"conn_lifetime" validate:"required"`
	ReporterConfig  aggregator.ReporterConfig `config:",squash"`
	ProfileName     string                    `config:"profile" validate:"required"`
	RunId           string                    `config:"run_id" validate:"required"`
	Hostname        string                    `config:"hostname" validate:"required"`
	IsDefineDDL     bool                      `config:"define_ddl"`
}

func NewDefaultConfiguration() Config {
	return Config{
		Address:         "127.0.0.1:9000",
		Database:        "pandora_stats",
		Username:        "default",
		Password:        "default",
		BatchSize:       500,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ReporterConfig: aggregator.ReporterConfig{
			SampleQueueSize: 3_000_000,
		},
		ProfileName: "default",
		RunId:       time.Now().String(),
		Hostname:    "localhost",
		IsDefineDDL: true,
	}
}
