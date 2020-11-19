## Certificate authority

The directory certificate-authority contains config files for creating a root certificate authority (CA),
a signing certificate authority, and various server certificates. 

#### Root certificate authority

The purpose of the root CA is to sign requests for signing CAs. The root is the top of the hierarchy
and is self signed.

The file root-ca.conf is an OpenSSl config file for creation of a root certificate authority. 

The command

```openssl req -new -config root-ca.conf -out root-ca/csr.pem -keyout root-ca/key.pem```

crestes the private key `key.pem` and a certificate signing request `csr.pem`


The next command issues a self signed certificate based on the request above.

```openssl ca -config root-ca.conf -selfsign -in root-ca/csr.pem -out root-ca/crt.pem -extensions root_ca_ext```


#### Signing certificate authority

The purpose of the signing CA is to sign requests for server certificates. 

The certificate signing request, and the key, for the signing CA is created as

```openssl req -new -config signing-ca.conf -out signing-ca/csr.pem -keyout signing-ca/key.pem```

The request is signed by the root as

```openssl ca -config root-ca.conf -in signing-ca/csr.pem -out signing-ca/crt.pem -extensions signing_ca_ext```


####  Server certificates 

The first server request is for a certificate where the server is identified by the DNS names, 
`www.example.com`, `server.example.com`,  `www.sub.example.com`, `support.example.com`.

The csr and key is created as

```openssl req -new -config dns-checked-server.conf -out certs/dns-checked-server.csr.pem -keyout certs/dns-checked-server.key.pem```

The signing by the signing-ca is

```openssl ca -config signing-ca.conf -in certs/dns-checked-server.csr.pem -out certs/dns-checked-server.crt.pem -extensions server_ext```

Similar commands are used to create a localhost server certificate and an IP address checked
certificate.

#### Client side trust of the root certificate.

The root certificate `root-ca/crt.pem` can be installed in clients. As a test it was installed in
Firefox.

#### Certificate chains

A server must present a certificate and a chain that allows the client to verify the certificate.
Since the client knows the root certificate but not the signing CA certificate, the server must
present a chain consisting of the signing CA and its own certificate, e.g.,

```cat dns-checked-server.crt.pem ../signing-ca/crt.pem > dns-chain.crt.pem```

Starting Nginx as a HTTPS server with the certificate `localhost-chain.crt.pem` and the key
`localhost-server.key.pem`, it is possible to connect Firefox with the root certificate to a HTTPS web page without warnings or exceptions.

## Connection to sync

The certificates presented here can be used to guide the implementation and testing of Sync over TLS.
