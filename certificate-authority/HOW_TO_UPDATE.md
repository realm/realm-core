Do this to recreate all the certificate files:

    openssl req -new -config root-ca.conf -out root-ca/csr.pem -keyout root-ca/key.pem
    openssl ca -config root-ca.conf -selfsign -in root-ca/csr.pem -out root-ca/crt.pem -extensions root_ca_ext

Answer yes twice. Remove all newly created files.

    openssl req -new -config signing-ca.conf -out signing-ca/csr.pem -keyout signing-ca/key.pem
    openssl ca -config root-ca.conf -in signing-ca/csr.pem -out signing-ca/crt.pem -extensions signing_ca_ext

Answer yes twice. Remove all newly created files.

    openssl req -new -config dns-checked-server.conf -out certs/dns-checked-server.csr.pem -keyout certs/dns-checked-server.key.pem -subj "/DC=www.example.com/CN=www.example.com"
    openssl ca -config signing-ca.conf -in certs/dns-checked-server.csr.pem -out certs/dns-checked-server.crt.pem -extensions server_ext -batch

Remove all newly created files.

    openssl req -new -config ip-server.conf -out certs/ip-server.csr.pem -keyout certs/ip-server.key.pem -subj "/DC=www.example.com/CN=www.example.com"
    openssl ca -config signing-ca.conf -in certs/ip-server.csr.pem -out certs/ip-server.crt.pem -extensions server_ext -batch

Remove all newly created files.

    openssl req -new -config localhost-server.conf -out certs/localhost-server.csr.pem -keyout certs/localhost-server.key.pem -subj "/DC=localhost/CN=localhost"
    openssl ca -config signing-ca.conf -in certs/localhost-server.csr.pem -out certs/localhost-server.crt.pem -extensions server_ext -batch

Remove all newly created files.

    cat certs/dns-checked-server.crt.pem signing-ca/crt.pem > certs/dns-chain.crt.pem
    cat certs/ip-server.crt.pem signing-ca/crt.pem > certs/ip-chain.crt.pem
    cat certs/localhost-server.crt.pem signing-ca/crt.pem > certs/localhost-chain.crt.pem

    openssl x509 -in certs/localhost-chain.crt.pem -outform der -out certs/localhost-chain.crt.cer
