package common

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"strings"
)

// RunServer runs an http.Server with the specified http.Handler forever. It
// will optionally enable TLS.
func RunServer(
	tlsConfig *TLSConfig,
	handler http.Handler,
	addr string,
	insecure bool,
) {
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	if insecure {
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	} else {
		cert, err := ioutil.ReadFile(tlsConfig.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)

		config := &tls.Config{
			ClientCAs:  certPool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		}
		config.BuildNameToCertificate()
		server.TLSConfig = config

		err = server.ListenAndServeTLS(
			tlsConfig.CertFile,
			tlsConfig.KeyFile,
		)
		if err != nil {
			panic(err)
		}
	}
}

// AcceptsMimeType returns whether the provided MIME type was mentioned in the
// Accept HTTP header in the http.Request.
func AcceptsMimeType(r *http.Request, mimeType string) bool {
	for _, accepts := range r.Header["Accept"] {
		for _, mime := range strings.Split(accepts, ",") {
			if strings.TrimSpace(mime) == mimeType {
				return true
			}
		}
	}
	return false
}
