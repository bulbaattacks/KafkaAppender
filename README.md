# KafkaAppender

Кастомный [Log4j2](https://logging.apache.org/log4j/2.x/) аппендер для отправки логов в [Apache Kafka](https://kafka.apache.org/).  
Библиотека минималистична: она только регистрирует аппендер и умеет принимать настройки продюсера через `log4j2.xml` вашего приложения.  
Все параметры Kafka конфигурируются **в приложении**, а не внутри самой библиотеки.

---

## ✨ Возможности
- Отправка логов напрямую в Kafka‑топик.
- Полная конфигурация через `log4j2.xml` (без жёстко прописанных параметров в коде).
- Поддержка любых `Layout` (по умолчанию `PatternLayout`).
- Корректное закрытие продюсера (`flush()` и `close()`).

---

## 📦 Установка

Добавьте зависимость в `pom.xml` вашего приложения:

```xml
<dependency>
  <groupId>io.github.bulbaattacks</groupId>
  <artifactId>kafka-appender</artifactId>
  <version>0.0.11</version>
</dependency>
