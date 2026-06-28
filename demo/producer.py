#!/usr/bin/env python3
"""Jeduler Demo Producer - submits jobs to the scheduler REST API."""

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import requests

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
log = logging.getLogger('jeduler-producer')

# Graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    log.info('Shutdown signal received, stopping...')
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Job type configurations
JOB_CONFIGS = {
    'vulnerability-scan': {
        'tenants': [101, 102, 103, 104, 105],
        'scan_types': ['full', 'quick', 'incremental'],
        'targets': [
            'https://app1.example.com',
            'https://app2.example.com',
            'https://api.example.com',
            'https://admin.example.com',
            'https://portal.example.com',
        ]
    },
    'code-analysis': {
        'tenants': [101, 102, 103],
        'languages': ['python', 'java', 'javascript', 'go'],
        'repos': [
            'github.com/org/frontend',
            'github.com/org/backend',
            'github.com/org/infra',
            'github.com/org/mobile',
        ]
    },
    'compliance-check': {
        'tenants': [101, 102, 103, 104, 105, 106],
        'standards': ['SOC2', 'HIPAA', 'PCI-DSS', 'GDPR'],
        'scopes': ['infrastructure', 'application', 'data'],
    }
}


def generate_job(job_type: str) -> dict:
    """Generate a random job payload for the given type."""
    config = JOB_CONFIGS[job_type]
    tenant = random.choice(config['tenants'])
    priority = random.choices([1, 2, 3, 5, 8, 10], weights=[1, 2, 3, 4, 3, 2])[0]

    if job_type == 'vulnerability-scan':
        scan_type = random.choice(config['scan_types'])
        target = random.choice(config['targets'])
        return {
            'jobName': job_type,
            'priority': priority,
            'tenant': tenant,
            'payload': {
                'targetUrl': target,
                'scanType': scan_type,
                'depth': random.randint(1, 5),
                'timeout': random.choice([60, 120, 300]),
            },
            'concurrencyControl': {
                'tenant': str(tenant),
                'scanType': scan_type,
            }
        }
    elif job_type == 'code-analysis':
        language = random.choice(config['languages'])
        repo = random.choice(config['repos'])
        return {
            'jobName': job_type,
            'priority': priority,
            'tenant': tenant,
            'payload': {
                'repository': repo,
                'language': language,
                'branch': random.choice(['main', 'develop', 'feature/new']),
                'analysisDepth': random.choice(['shallow', 'deep']),
            },
            'concurrencyControl': {
                'tenant': str(tenant),
            }
        }
    else:  # compliance-check
        standard = random.choice(config['standards'])
        scope = random.choice(config['scopes'])
        return {
            'jobName': job_type,
            'priority': priority,
            'tenant': tenant,
            'payload': {
                'standard': standard,
                'scope': scope,
                'includeEvidence': random.choice([True, False]),
            },
            'concurrencyControl': {}
        }


def submit_job(base_url: str, job: dict) -> dict:
    """Submit a job to the scheduler."""
    url = f'{base_url}/api/v1/jobs'
    start = time.time()
    try:
        response = requests.post(url, json=job, timeout=10)
        latency_ms = (time.time() - start) * 1000

        if response.status_code == 201:
            result = response.json()
            log.info(json.dumps({
                'event': 'job_submitted',
                'jobId': result.get('jobId'),
                'jobName': job['jobName'],
                'tenant': job['tenant'],
                'priority': job['priority'],
                'latency_ms': round(latency_ms, 1),
            }))
            return result
        elif response.status_code == 409:
            log.warning(json.dumps({
                'event': 'duplicate_job',
                'jobName': job['jobName'],
                'tenant': job['tenant'],
                'latency_ms': round(latency_ms, 1),
            }))
            return None
        else:
            log.error(json.dumps({
                'event': 'submit_failed',
                'status': response.status_code,
                'body': response.text[:200],
                'latency_ms': round(latency_ms, 1),
            }))
            return None
    except requests.exceptions.RequestException as e:
        log.error(json.dumps({'event': 'submit_error', 'error': str(e)}))
        return None


def run_steady(base_url: str, rate: float, job_types: list):
    """Submit jobs at a steady rate."""
    interval = 1.0 / rate
    submitted = 0

    log.info(f'Starting steady mode: {rate} jobs/sec, types={job_types}')

    while running:
        job_type = random.choice(job_types)
        job = generate_job(job_type)
        submit_job(base_url, job)
        submitted += 1

        sleep_time = interval + random.uniform(-interval * 0.2, interval * 0.2)
        time.sleep(max(0.1, sleep_time))

    log.info(f'Shutdown complete. Total submitted: {submitted}')


def run_burst(base_url: str, rate: float, job_types: list):
    """Submit jobs in bursts with quiet periods."""
    submitted = 0

    log.info(f'Starting burst mode: base_rate={rate}, types={job_types}')

    while running:
        # Burst phase: 10-30 jobs in quick succession
        burst_size = random.randint(10, 30)
        log.info(json.dumps({'event': 'burst_start', 'size': burst_size}))

        for _ in range(burst_size):
            if not running:
                break
            job_type = random.choice(job_types)
            job = generate_job(job_type)
            submit_job(base_url, job)
            submitted += 1
            time.sleep(random.uniform(0.05, 0.2))

        if not running:
            break

        # Quiet phase: 5-15 seconds
        quiet_time = random.uniform(5, 15)
        log.info(json.dumps({'event': 'quiet_period', 'duration_s': round(quiet_time, 1)}))

        end_time = time.time() + quiet_time
        while running and time.time() < end_time:
            time.sleep(0.5)

    log.info(f'Shutdown complete. Total submitted: {submitted}')


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
    parser = argparse.ArgumentParser(description='Jeduler Demo Producer')
    parser.add_argument('--url', default=os.getenv('SCHEDULER_URL', 'http://localhost:8080'),
                       help='Scheduler base URL')
    parser.add_argument('--mode', default=os.getenv('MODE', 'steady'),
                       choices=['steady', 'burst'], help='Submission mode')
    parser.add_argument('--rate', type=float, default=float(os.getenv('RATE', '2')),
                       help='Jobs per second (steady mode)')
    parser.add_argument('--job-types', default=os.getenv('JOB_TYPES', 'vulnerability-scan,code-analysis,compliance-check'),
                       help='Comma-separated job types')

    args = parser.parse_args()
    job_types = [t.strip() for t in args.job_types.split(',')]

    # Validate job types
    valid_types = set(JOB_CONFIGS.keys())
    for jt in job_types:
        if jt not in valid_types:
            log.error(f'Invalid job type: {jt}. Valid: {valid_types}')
            sys.exit(1)

    # Wait for scheduler
    if not wait_for_scheduler(args.url):
        sys.exit(1)

    # Run
    if args.mode == 'steady':
        run_steady(args.url, args.rate, job_types)
    else:
        run_burst(args.url, args.rate, job_types)


if __name__ == '__main__':
    main()
