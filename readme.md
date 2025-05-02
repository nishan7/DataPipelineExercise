# Kafka Producer and Consumer in Python

This project demonstrates a simple implementation of a Kafka producer and consumer using Python and the `confluent-kafka` library.

## Project Structure

- `constants.py`: Contains configuration constants such as Kafka broker, topic name, and threshold values.
- `produce.py`: Implements a Kafka producer that sends random log messages to a specified Kafka topic.
- `consume.py`: Implements a Kafka consumer that reads messages from the topic and validates them based on certain criteria.

## Prerequisites

- Python 3.7 or higher
- Kafka broker running locally or remotely
- `confluent-kafka` library installed

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>