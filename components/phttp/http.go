// Copyright (c) 2017 Yandex LLC. All rights reserved.
// Use of this source code is governed by a MPL 2.0
// license that can be found in the LICENSE file.
// Author: Vladimir Skipor <skipor@yandex-team.ru>

package phttp

import (
	"net/http"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ClientGunConfig struct {
	Target string `validate:"endpoint,required"`
	SSL    bool
	Base   BaseGunConfig `config:",squash"`
}

type HTTPGunConfig struct {
	Gun    ClientGunConfig `config:",squash"`
	Client ClientConfig    `config:",squash"`
}

type HTTP2GunConfig struct {
	Gun    ClientGunConfig `config:",squash"`
	Client ClientConfig    `config:",squash"`
}

func NewHTTPGun(conf HTTPGunConfig, answLog *zap.Logger) *HTTPGun {
	transport := NewTransport(conf.Client.Transport, NewDialer(conf.Client.Dialer).DialContext)
	client := newClient(transport, conf.Client.Redirect)
	return NewClientGun(client, conf.Gun, answLog)
}

// NewHTTP2Gun return simple HTTP/2 gun that can shoot sequentially through one connection.
func NewHTTP2Gun(conf HTTP2GunConfig, answLog *zap.Logger) (*HTTPGun, error) {
	if !conf.Gun.SSL {
		// Open issue on github if you really need this feature.
		return nil, errors.New("HTTP/2.0 over TCP is not supported. Please leave SSL option true by default.")
	}
	transport := NewHTTP2Transport(conf.Client.Transport, NewDialer(conf.Client.Dialer).DialContext)
	client := newClient(transport, conf.Client.Redirect)
	// Will panic and cancel shooting whet target doesn't support HTTP/2.
	client = &panicOnHTTP1Client{client}
	return NewClientGun(client, conf.Gun, answLog), nil
}

func NewClientGun(client Client, conf ClientGunConfig, answLog *zap.Logger) *HTTPGun {
	scheme := "http"
	if conf.SSL {
		scheme = "https"
	}
	var g HTTPGun
	g = HTTPGun{
		BaseGun: BaseGun{
			Config: conf.Base,
			Do:     g.Do,
			OnClose: func() error {
				client.CloseIdleConnections()
				return nil
			},
			AnswLog: answLog,
		},
		scheme: scheme,
		target: conf.Target,
		client: client,
	}
	return &g
}

type HTTPGun struct {
	BaseGun
	scheme string
	target string
	client Client
}

var _ Gun = (*HTTPGun)(nil)

func (g *HTTPGun) Do(req *http.Request) (*http.Response, error) {
	req.Host = req.URL.Host
	req.URL.Host = g.target
	req.URL.Scheme = g.scheme
	return g.client.Do(req)
}

func DefaultHTTPGunConfig() HTTPGunConfig {
	return HTTPGunConfig{
		Gun:    DefaultClientGunConfig(),
		Client: DefaultClientConfig(),
	}
}

func DefaultHTTP2GunConfig() HTTP2GunConfig {
	conf := HTTP2GunConfig{
		Client: DefaultClientConfig(),
		Gun:    DefaultClientGunConfig(),
	}
	conf.Gun.SSL = true
	return conf
}

func DefaultClientGunConfig() ClientGunConfig {
	return ClientGunConfig{
		SSL:  false,
		Base: DefaultBaseGunConfig(),
	}
}
