FROM rabbitmq:4.0.1-management

COPY .ci/conf/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
COPY .ci/conf/enabled_plugins /etc/rabbitmq/enabled_plugins

COPY .ci/certs /etc/rabbitmq/certs
