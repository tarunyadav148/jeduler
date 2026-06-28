#!/usr/bin/env python3
"""Jeduler Demo Consumer - consumes jobs from Kafka and simulates execution."""

import argparse
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
log = logging.getLogger('jeduler-consumer')

# Graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    log.info('Shutdown signal received, finishing current jobs...')
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class JobProcessor:
    """Processes jobs consumed from Kafka."""

    def __init__(self, scheduler_url: str, heartbeat_interval: int,
                 failure_rate: float, min_time: int, max_time: int):
        self.scheduler_url = scheduler_url
        self.heartbeat_interval = heartbeat_interval
        self.failure_rate = failure_rate
        self.min_time = min_time
        self.max_time = max_time
        self.hostname = os.getenv('HOSTNAME', 'consumer-1')

    def process_job(self, message: dict):
        """Process a single job message."""
        job_id = message.get('jobId')
        job_name = message.get('jobName')
        tenant = message.get('tenant')

        log.info(json.dumps({
            'event': 'job_received',
            'jobId': job_id,
            'jobName': job_name,
            'tenant': tenant,
        }))

        # Report PROCESSING status
        self._report_status(job_id, 'PROCESSING')

        # Simulate work with heartbeats
        processing_time = random.uniform(self.min_time, self.max_time)
        start_time = time.time()
        elapsed = 0

        while elapsed < processing_time and running:
            # Send heartbeat
            progress = int((elapsed / processing_time) * 100)
            self._send_heartbeat(job_id, progress)

            # Sleep for heartbeat interval or remaining time
            sleep_time = min(self.heartbeat_interval, processing_time - elapsed)
            time.sleep(sleep_time)
            elapsed = time.time() - start_time

        if not running:
            # Shutting down mid-job - let heartbeat expire naturally
            log.warning(json.dumps({'event': 'job_interrupted', 'jobId': job_id}))
            return

        # Determine success/failure
        if random.random() < self.failure_rate:
            reason = random.choice([
                'Connection timeout to target host',
                'Out of memory during analysis',
                'Target returned HTTP 500',
                'Scan interrupted by rate limiting',
                'Resource quota exceeded',
            ])
            self._report_status(job_id, 'FAILED', reason=reason)
            log.info(json.dumps({
                'event': 'job_failed',
                'jobId': job_id,
                'jobName': job_name,
                'reason': reason,
                'duration_s': round(processing_time, 1),
            }))
        else:
            result = self._generate_result(job_name)
            self._report_status(job_id, 'SUCCESSFUL', result=result)
            log.info(json.dumps({
                'event': 'job_completed',
                'jobId': job_id,
                'jobName': job_name,
                'duration_s': round(processing_time, 1),
            }))

    def _report_status(self, job_id: int, status: str, reason: str = None, result: dict = None):
        """Report job status back to the scheduler."""
        url = f'{self.scheduler_url}/api/v1/jobs/{job_id}/status'
        body = {
            'status': status,
            'source': {
                'app': 'demo-consumer',
                'host': self.hostname,
                'consumedAt': datetime.now(timezone.utc).isoformat(),
            }
        }
        if reason:
            body['reason'] = reason
        if result:
            body['result'] = result

        try:
            response = requests.post(url, json=body, timeout=10)
            if response.status_code != 200:
                log.error(json.dumps({
                    'event': 'status_report_failed',
                    'jobId': job_id,
                    'status': status,
                    'http_status': response.status_code,
                    'body': response.text[:200],
                }))
        except requests.exceptions.RequestException as e:
            log.error(json.dumps({
                'event': 'status_report_error',
                'jobId': job_id,
                'error': str(e),
            }))

    def _send_heartbeat(self, job_id: int, progress: int):
        """Send heartbeat to the scheduler."""
        url = f'{self.scheduler_url}/api/v1/jobs/{job_id}/heartbeat'
        body = {
            'progress': progress,
            'message': f'Processing... {progress}%'
        }

        try:
            requests.post(url, json=body, timeout=5)
        except requests.exceptions.RequestException as e:
            log.warning(json.dumps({
                'event': 'heartbeat_failed',
                'jobId': job_id,
                'error': str(e),
            }))

    def _generate_result(self, job_name: str) -> dict:
        """Generate a simulated result based on job type."""
        if job_name == 'vulnerability-scan':
            return {
                'vulnerabilitiesFound': random.randint(0, 15),
                'criticalCount': random.randint(0, 3),
                'highCount': random.randint(0, 5),
                'scanDuration': round(random.uniform(5, 30), 1),
            }
        elif job_name == 'code-analysis':
            return {
                'issuesFound': random.randint(0, 50),
                'linesAnalyzed': random.randint(1000, 100000),
                'complexity': round(random.uniform(1.0, 10.0), 2),
            }
        else:  # compliance-check
            return {
                'controlsPassed': random.randint(50, 100),
                'controlsFailed': random.randint(0, 10),
                'complianceScore': round(random.uniform(70, 100), 1),
            }


def consume_topics(kafka_brokers: str, job_types: list, processor: JobProcessor):
    """Main consumer loop."""
    topics = [f'job-dispatch.{jt}' for jt in job_types]
    group_id = 'demo-workers'

    conf = {
        'bootstrap.servers': kafka_brokers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000,
        'session.timeout.ms': 45000,
        'heartbeat.interval.ms': 10000,
    }

    consumer = Consumer(conf)
    consumer.subscribe(topics)

    log.info(f'Subscribed to topics: {topics}')

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f'Consumer error: {msg.error()}')
                continue

            # Parse message
            try:
                value = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.error(f'Failed to parse message: {e}')
                consumer.commit(asynchronous=False)
                continue

            # Process the job
            processor.process_job(value)

            # Commit offset after processing
            consumer.commit(asynchronous=False)

    except KafkaException as e:
        log.error(f'Kafka error: {e}')
    finally:
        consumer.close()
        log.info('Consumer closed')


def wait_for_kafka(brokers: str, max_wait: int = 120):
    """Wait for Kafka to be available."""
    log.info(f'Waiting for Kafka at {brokers}...')
    start = time.time()

    while time.time() - start < max_wait:
        try:
            conf = {
                'bootstrap.servers': brokers,
                'group.id': 'health-check',
                'session.timeout.ms': 6000,
            }
            c = Consumer(conf)
            metadata = c.list_topics(timeout=5)
            c.close()
            log.info(f'Kafka is ready. Topics: {list(metadata.topics.keys())}')
            return True
        except Exception:
            pass
        time.sleep(2)

    log.error(f'Kafka not ready after {max_wait}s')
    return False


def wait_for_scheduler(base_url: str, max_wait: int = 120):
    """Wait for the scheduler to be healthy."""
    log.info(f'Waiting for scheduler at {base_url}...')
    start = time.time()

    while time.time() - start < max_wait:
        try:
            response = requests.get(f'{base_url}/actuator/health', timeout=5)
            if response.status_code == 200:
                log.info('Scheduler is healthy!')
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(2)

    log.error(f'Scheduler not ready after {max_wait}s')
    return False


def main():
    parser = argparse.ArgumentParser(description='Jeduler Demo Consumer')
    parser.add_argument('--kafka-brokers', default=os.getenv('KAFKA_BROKERS', 'localhost:9092'),
                       help='Kafka bootstrap servers')
    parser.add_argument('--scheduler-url', default=os.getenv('SCHEDULER_URL', 'http://localhost:8080'),
                       help='Scheduler base URL')
    parser.add_argument('--job-types', default=os.getenv('JOB_TYPES', 'vulnerability-scan,code-analysis,compliance-check'),
                       help='Comma-separated job types to consume')
    parser.add_argument('--heartbeat-interval', type=int,
                       default=int(os.getenv('HEARTBEAT_INTERVAL', '15')),
                       help='Heartbeat interval in seconds')
    parser.add_argument('--failure-rate', type=float,
                       default=float(os.getenv('FAILURE_RATE', '0.1')),
                       help='Simulated failure rate (0.0-1.0)')
    parser.add_argument('--min-time', type=int,
                       default=int(os.getenv('MIN_PROCESSING_TIME', '5')),
                       help='Minimum processing time (seconds)')
    parser.add_argument('--max-time', type=int,
                       default=int(os.getenv('MAX_PROCESSING_TIME', '30')),
                       help='Maximum processing time (seconds)')

    args = parser.parse_args()
    job_types = [t.strip() for t in args.job_types.split(',')]

    # Wait for dependencies
    if not wait_for_kafka(args.kafka_brokers):
        sys.exit(1)
    if not wait_for_scheduler(args.scheduler_url):
        sys.exit(1)

    # Create processor
    processor = JobProcessor(
        scheduler_url=args.scheduler_url,
        heartbeat_interval=args.heartbeat_interval,
        failure_rate=args.failure_rate,
        min_time=args.min_time,
        max_time=args.max_time,
    )

    # Start consuming
    consume_topics(args.kafka_brokers, job_types, processor)


if __name__ == '__main__':
    main()
