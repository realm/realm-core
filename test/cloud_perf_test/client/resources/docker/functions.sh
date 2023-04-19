get_local_image_name()
{
    namespace="$1"
    tag="$2"
    repository="test/sync-perf-$namespace-client"
    printf "%s:%s" "$repository" "$tag"
}

get_remote_image_name()
{
    namespace="$1"
    tag="$2"
    repository="012067661104.dkr.ecr.eu-west-1.amazonaws.com/test/sync-perf-$namespace-client"
    printf "%s:%s" "$repository" "$tag"
}
