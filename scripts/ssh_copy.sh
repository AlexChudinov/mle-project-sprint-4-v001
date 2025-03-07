USER=mle-user
VM_IP=84.201.139.92

function sync_dir_from() {
    LOCAL_DIR=$1
    REMOTE_DIR=$2
    rsync -v --progress ${USER}@${VM_IP}:${REMOTE_DIR} ${LOCAL_DIR}
}

function sync_dir_to() {
    LOCAL_DIR=$1
    REMOTE_DIR=$2
    rsync -v --progress ${LOCAL_DIR} ${USER}@${VM_IP}:${REMOTE_DIR}
}
case $1 in
    --to)
        sync_dir_to $2 $3
        ;;
    --from)
        sync_dir_from $2 $3
        ;;
    *)
        echo "unsupported parameter: '${1}'"
        ;;
esac
