set -e

case $1 in
    "->")
        aws --endpoint-url https://storage.yandexcloud.net \
            --profile yandex_cloud \
            s3 sync data s3://s3-student-mle-20240919-daec520330/recsys/data

        aws --endpoint-url https://storage.yandexcloud.net \
            --profile yandex_cloud \
            s3 sync recommendations s3://s3-student-mle-20240919-daec520330/recsys/recommendations
    ;;
    "<-")
        aws --endpoint-url https://storage.yandexcloud.net \
            --profile yandex_cloud \
            s3 sync s3://s3-student-mle-20240919-daec520330/recsys/recommendations recommendations

        aws --endpoint-url https://storage.yandexcloud.net \
            --profile yandex_cloud \
            s3 sync s3://s3-student-mle-20240919-daec520330/recsys/data data
    ;;
    *)
        echo error: unknown command $1
        exit 1
esac

exit 0
