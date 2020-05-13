Steps to deploy:
1. Choose Kubernetes namespace `<namespace>`. Can be `ks` for Kristian Spangsege.
2. Choose profile `<profile>`. Can be `fanout-send`, `fanout-recv`, `fanout-psend`, or `fanout-precv`.
3. Choose server base URL `<server base url>`. Can be `realms://kristian.arena.realmlab.net/iter_1` for Kristian Spangsege.
4. Build a Docker image with the test client by running `sh rebuild-docker-image.sh <namespace>`.
5. Deploy the test client into Kubernetes cluster by running `sh start.sh <namespace> <profile> <server base url>`.

Tested with Helm 2.9.1 (https://github.com/helm/helm/releases/tag/v2.9.1).

The fan-out dashboard is currently available on https://stats.realmlab.net/d/lJYvjQBiz/fan-out?orgId=1.

To gain access to the Arena cluster, update your ~/.kube/config by running the script at https://arena.k8s.realmlab.net/kubeconfig.

Kubernetes web interface: https://arena.k8s.realmlab.net/#!/deployment.
