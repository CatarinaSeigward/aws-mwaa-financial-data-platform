"""
Slack Notification Lambda Handler
==================================
AWS Lambda function for sending pipeline alerts to Slack.
Handles various event types: validation failures, pipeline success, errors.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret(secret_name: str) -> str:
    """
    Retrieve secret from AWS Secrets Manager.
    
    Args:
        secret_name: Name of the secret
        
    Returns:
        Secret value
    """
    client = boto3.client("secretsmanager")
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret.get("webhook_url", "")
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {e}")
        raise


def format_validation_failure(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format Slack message for validation failure.
    
    Args:
        event: Event payload with validation details
        
    Returns:
        Slack message payload
    """
    validation_results = event.get("validation_results", {})
    failed_expectations = validation_results.get("failed_expectations", [])
    
    # Build failure details
    failure_blocks = []
    for i, failure in enumerate(failed_expectations[:5], 1):
        exp_type = failure.get("expectation_type", "Unknown")
        details = failure.get("details", {})
        failure_blocks.append(
            f"*{i}. {exp_type}*\n"
            f"```{json.dumps(details, indent=2)[:200]}```"
        )
    
    if len(failed_expectations) > 5:
        failure_blocks.append(f"_...and {len(failed_expectations) - 5} more failures_")
    
    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Data Validation Failed",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\n{event.get('dag_id', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Date:*\n{event.get('execution_date', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Run ID:*\n{event.get('run_id', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Failed Expectations:*\n{len(failed_expectations)}"
                    }
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Failed Validation Rules:*\n" + "\n".join(failure_blocks)
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Pipeline blocked at validation gate • {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View in Airflow",
                            "emoji": True
                        },
                        "url": f"https://mwaa.{os.getenv('AWS_REGION', 'us-east-1')}.amazonaws.com/home"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Logs",
                            "emoji": True
                        },
                        "url": f"https://console.aws.amazon.com/cloudwatch/home?region={os.getenv('AWS_REGION', 'us-east-1')}#logsV2:log-groups"
                    }
                ]
            }
        ]
    }


def format_pipeline_success(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format Slack message for pipeline success.
    
    Args:
        event: Event payload
        
    Returns:
        Slack message payload
    """
    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ Pipeline Completed Successfully",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\n{event.get('dag_id', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Date:*\n{event.get('execution_date', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Run ID:*\n{event.get('run_id', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\nAll stages completed"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Data pipeline completed • {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            }
        ]
    }


def format_pipeline_error(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format Slack message for pipeline error.
    
    Args:
        event: Event payload with error details
        
    Returns:
        Slack message payload
    """
    error_message = event.get("error_message", "Unknown error")
    task_id = event.get("task_id", "N/A")
    
    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "❌ Pipeline Error",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\n{event.get('dag_id', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Task:*\n{task_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Date:*\n{event.get('execution_date', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Run ID:*\n{event.get('run_id', 'N/A')}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error Message:*\n```{error_message[:500]}```"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Error occurred at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Task Logs",
                            "emoji": True
                        },
                        "style": "danger",
                        "url": f"https://console.aws.amazon.com/cloudwatch/home"
                    }
                ]
            }
        ]
    }


def format_custom_alert(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format Slack message for custom alerts.
    
    Args:
        event: Event payload
        
    Returns:
        Slack message payload
    """
    title = event.get("title", "Alert")
    message = event.get("message", "No message provided")
    severity = event.get("severity", "info")
    
    emoji_map = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "critical": "🚨"
    }
    emoji = emoji_map.get(severity, "📢")
    
    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Severity: {severity.upper()} • {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            }
        ]
    }


def send_slack_message(webhook_url: str, payload: Dict[str, Any]) -> bool:
    """
    Send message to Slack webhook.
    
    Args:
        webhook_url: Slack webhook URL
        payload: Message payload
        
    Returns:
        True if successful
    """
    try:
        data = json.dumps(payload).encode("utf-8")
        
        request = Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        
        with urlopen(request, timeout=10) as response:
            if response.status == 200:
                logger.info("Slack message sent successfully")
                return True
            else:
                logger.error(f"Slack returned status {response.status}")
                return False
                
    except HTTPError as e:
        logger.error(f"HTTP error sending Slack message: {e.code} - {e.reason}")
        return False
    except URLError as e:
        logger.error(f"URL error sending Slack message: {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Failed to send Slack message: {e}")
        return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda entry point.
    
    Args:
        event: Lambda event payload
        context: Lambda context
        
    Returns:
        Response object
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Get webhook URL
    webhook_secret_name = os.getenv(
        "SLACK_WEBHOOK_SECRET_NAME",
        "financial-platform/slack-webhook"
    )
    
    try:
        webhook_url = get_secret(webhook_secret_name)
    except Exception as e:
        logger.error(f"Failed to get webhook URL: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to retrieve Slack webhook"})
        }
    
    # Determine event type and format message
    event_type = event.get("event_type", "CUSTOM")
    
    formatters = {
        "VALIDATION_FAILURE": format_validation_failure,
        "PIPELINE_SUCCESS": format_pipeline_success,
        "PIPELINE_ERROR": format_pipeline_error,
        "CUSTOM": format_custom_alert,
    }
    
    formatter = formatters.get(event_type, format_custom_alert)
    payload = formatter(event)
    
    # Send message
    success = send_slack_message(webhook_url, payload)
    
    if success:
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Notification sent successfully"})
        }
    else:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to send notification"})
        }


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {
        "event_type": "VALIDATION_FAILURE",
        "dag_id": "financial_data_pipeline",
        "execution_date": "2024-01-15",
        "run_id": "manual__2024-01-15T00:00:00+00:00",
        "validation_results": {
            "overall_success": False,
            "failed_expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "details": {"column": "high_price", "min": 0, "violations": 5}
                }
            ]
        }
    }
    
    # Format test message
    payload = format_validation_failure(test_event)
    print(json.dumps(payload, indent=2))
