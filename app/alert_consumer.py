"""Consumidor final dos alertas. Roda com a identidade de LEITURA (sa-payments-reader),
que só tem permissão de ler o tópico fraud_alerts e o subject fraud_alerts-value."""
import argparse
import json
import time
from pathlib import Path

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from common import ROOT, require


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=90, help="segundos escutando (0 = para sempre)")
    parser.add_argument("--group", default="fraud-alerts-consumer")
    args = parser.parse_args()

    env = require("BOOTSTRAP", "READER_API_KEY", "READER_API_SECRET",
                  "SR_URL", "READER_SR_API_KEY", "READER_SR_API_SECRET")
    sr = SchemaRegistryClient({
        "url": env["SR_URL"],
        "basic.auth.user.info": f"{env['READER_SR_API_KEY']}:{env['READER_SR_API_SECRET']}",
    })
    consumer = DeserializingConsumer({
        "bootstrap.servers": env["BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": env["READER_API_KEY"],
        "sasl.password": env["READER_API_SECRET"],
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "value.deserializer": AvroDeserializer(sr),
    })
    consumer.subscribe(["fraud_alerts"])

    out = ROOT / "evidence" / "04_alertas_consumidos.jsonl"
    deadline = time.time() + args.duration if args.duration else float("inf")
    count = 0
    print(f"Escutando fraud_alerts como {env['READER_API_KEY']} (grupo {args.group})...")
    try:
        with out.open("a", encoding="utf-8") as fh:
            while time.time() < deadline:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"erro: {msg.error()}")
                    continue
                alert = msg.value()
                count += 1
                line = json.dumps(alert, default=str, ensure_ascii=False)
                fh.write(line + "\n")
                print(f"🚨 [{alert.get('severity')}] {alert.get('rule_id')} cartão={alert.get('card_id')} "
                      f"qtd={alert.get('txn_count')} total=R$ {alert.get('total_amount') or 0:.2f} "
                      f"às {alert.get('alert_time')}")
    finally:
        consumer.close()
    print(f"\n{count} alerta(s) consumido(s). Salvos em {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
