from __future__ import annotations

from os import getenv
from typing import Any, Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message


def get_default_env(name: str) -> str:
    """
    Look for values in enviroment file.
    :param name: The name to look in env
    :type name: str
    :return: Value given the name
    :rtype: str
    """
    value: Optional[str] = getenv(name)
    if value is None:
        raise ValueError(f"The default value for {name} was not found in ENV FILE")
    return value


class KafkaConnectionConf:
    __version__ = "1.0.0"

    def __init__(
        self,
        *,
        session_timeout: int = 30000,
        auto_offset_reset: str = "lastest",  # reads only new messages
        security_protocol: str = "SASL_SSL",  # "PLAINTEXT"
        sasl_mechanisms: str = "PLAIN",
    ) -> None:
        self.__config = {
            "session.timeout.ms": session_timeout,
            "bootstrap.servers": get_default_env("KAFKA_SERVERS"),
            "sasl.username": get_default_env("KAFKA_USERNAME"),
            "sasl.password": get_default_env("KAFKA_PASSWORD"),
            "security.protocol": security_protocol,
            "ssl.endpoint.identification.algorithm": "none",
            "sasl.mechanisms": sasl_mechanisms,
            "auto.offset.reset": auto_offset_reset,
        }
        if sasl_mechanisms != "PLAIN" or security_protocol != "PLAINTEXT":
            # ssl_congif = {
            #     "ssl.endpoint.identification.algorithm": "?",
            #     "ssl.ca.location": "server.cer.pem",
            #     "ssl.key.location": "client.key.pem",
            #     "ssl.certificate.location": "client.cer.pem",
            # }
            # self.security_config.update(ssl_congif)
            raise ValueError(
                "Only PLAINTEXT mechanism is supported for now. "
                "Please check your KAFKA configuration."
            )

    def get_config(self) -> dict:
        """
        Returns the configuration dictionary for the Kafka connection.
        :return: Configuration dictionary
        :rtype: dict
        """
        return self.__config


class KafkaConsumer:
    __version__ = "1.0.0"

    def __init__(
        self, connection_conf: KafkaConnectionConf, *, group_id: Optional[str] = None
    ) -> None:
        self._connection_conf = connection_conf
        self._consumer_conf = {
            "group.id": group_id or get_default_env("KAFKA_GROUP_ID"),
        }
        self._consumer_conf.update(self._connection_conf.get_config())
        self._consumer = Consumer(self._consumer_conf)

    def subscribe(self, topic: str) -> None:
        self._consumer.subscribe([topic])

    def poll(self, timeout: float = 1.0) -> Optional[str]:
        """
        Polls for a message from the Kafka topic.
        :param timeout: Time in seconds to wait for a message, defaults to 1.0
        :type timeout: float, optional
        :return: The message value as a dictionary if a message is received, otherwise None
        :rtype: Optional[str]
        """
        msg: Message = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                return None
            else:
                # Error
                raise KafkaException(msg.error())
        return msg.value().decode("utf-8")

    def close(self) -> None:
        self._consumer.close()


class KafkaProducer:
    __version__ = "1.0.0"

    def __init__(
        self, connection_conf: KafkaConnectionConf, *, client_id: Optional[str] = None
    ) -> None:
        self._connection_conf = connection_conf
        self._producer_conf = {
            "client.id": client_id or get_default_env("KAFKA_CLIENT_ID"),
        }
        self._producer_conf.update(self._connection_conf.get_config())
        self._producer = Consumer(self._producer_conf)

    def produce(
        self,
        value: str,
        *,
        topic: str,
        headers: Optional[dict[str, str]] = None,
        callback_fun: Optional[Callable[[Exception, Any], None]] = None,
    ) -> None:
        """
        Produces a message to the specified Kafka topic.
        :param topic: The Kafka topic to produce the message to
        :type topic: str
        :param value: The message value to be sent
        :type value: str
        """
        self._producer.produce(
            topic=topic,
            value=value.encode("utf-8"),
            headers=(
                {k: v.encode("utf-8") for k, v in headers.items()} if headers else None
            ),
            callback=callback_fun,
        )
        self._producer.flush()
