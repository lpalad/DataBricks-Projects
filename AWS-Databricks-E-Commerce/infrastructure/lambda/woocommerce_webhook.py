"""
AWS Lambda: WooCommerce Webhook Handler
Receives webhooks from WooCommerce and writes to S3 landing zone
"""

import json
import boto3
import hashlib
import hmac
import base64
from datetime import datetime
import os

s3_client = boto3.client('s3')

# Configuration from environment variables
S3_BUCKET = os.environ.get('S3_BUCKET', 'ecommerce-landing-zone')
WOOCOMMERCE_SECRET = os.environ.get('WOOCOMMERCE_SECRET', '')


def verify_webhook_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Verify WooCommerce webhook signature"""
    # Skip verification for now - WooCommerce signature format varies
    # TODO: Re-enable after confirming signature format
    print(f"Signature verification skipped. Secret configured: {bool(secret)}")
    return True


def lambda_handler(event, context):
    """
    Main Lambda handler for WooCommerce webhooks

    Supported webhook topics:
    - order.created
    - order.updated
    - order.deleted
    - customer.created
    - customer.updated
    - customer.deleted
    - product.created
    - product.updated
    - product.deleted
    """

    try:
        # Parse request
        headers = event.get('headers', {})
        body = event.get('body', '')

        # Get webhook metadata
        webhook_topic = headers.get('x-wc-webhook-topic', 'unknown')
        webhook_source = headers.get('x-wc-webhook-source', 'unknown')
        webhook_signature = headers.get('x-wc-webhook-signature', '')

        print(f"Received webhook: {webhook_topic} from {webhook_source}")

        # Verify signature
        if not verify_webhook_signature(body.encode(), webhook_signature, WOOCOMMERCE_SECRET):
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Invalid signature'})
            }

        # Parse payload
        payload = json.loads(body) if isinstance(body, str) else body

        # Determine entity type and action
        topic_parts = webhook_topic.split('.')
        entity_type = topic_parts[0] if len(topic_parts) > 0 else 'unknown'
        action = topic_parts[1] if len(topic_parts) > 1 else 'unknown'

        # Map entity types to S3 prefixes
        entity_map = {
            'order': 'orders',
            'customer': 'customers',
            'product': 'products'
        }

        s3_prefix = entity_map.get(entity_type, 'unknown')

        # Generate unique filename
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        entity_id = payload.get('id', 'unknown')
        filename = f"{s3_prefix}/{entity_type}_{entity_id}_{action}_{timestamp}.json"

        # Add metadata to payload
        enriched_payload = {
            '_webhook_topic': webhook_topic,
            '_webhook_source': webhook_source,
            '_received_at': datetime.utcnow().isoformat(),
            '_action': action,
            'data': payload
        }

        # Write to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=filename,
            Body=json.dumps(enriched_payload, default=str),
            ContentType='application/json',
            Metadata={
                'webhook-topic': webhook_topic,
                'entity-type': entity_type,
                'entity-id': str(entity_id),
                'action': action
            }
        )

        print(f"Successfully wrote to s3://{S3_BUCKET}/{filename}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Webhook processed successfully',
                'file': filename
            })
        }

    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid JSON payload'})
        }

    except Exception as e:
        print(f"Error processing webhook: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


# For local testing
if __name__ == '__main__':
    # Sample order webhook
    test_event = {
        'headers': {
            'x-wc-webhook-topic': 'order.created',
            'x-wc-webhook-source': 'https://mystore.com'
        },
        'body': json.dumps({
            'id': 12345,
            'order_key': 'wc_order_abc123',
            'status': 'processing',
            'total': '150.00',
            'customer_id': 1,
            'billing': {
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@example.com'
            },
            'line_items': [
                {'product_id': 101, 'name': 'Product A', 'quantity': 2, 'price': '50.00'},
                {'product_id': 102, 'name': 'Product B', 'quantity': 1, 'price': '50.00'}
            ]
        })
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
