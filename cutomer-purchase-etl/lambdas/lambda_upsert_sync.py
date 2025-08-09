import pymysql
import os

def lambda_handler(event, context):
    conn = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_NAME'],
        connect_timeout=10
    )

    merge_sql = ('\n'
                 '    INSERT INTO customer_purchase_summary AS target\n'
                 '        (customer_id, full_name, email, total_spent_aud, first_purchase_date, last_purchase_date)\n'
                 '    SELECT\n'
                 '        s.customer_id,\n'
                 '        s.full_name,\n'
                 '        s.email,\n'
                 '        s.total_spent_aud,\n'
                 '        s.first_purchase_date,\n'
                 '        s.last_purchase_date\n'
                 '    FROM\n'
                 '        customer_purchase_summary_staging s\n'
                 '    ON DUPLICATE KEY UPDATE\n'
                 '        total_spent_aud = target.total_spent_aud + s.total_spent_aud,\n'
                 '        first_purchase_date = LEAST(target.first_purchase_date, s.first_purchase_date),\n'
                 '        last_purchase_date = GREATEST(target.last_purchase_date, s.last_purchase_date);\n'
                 '    ')

    with conn.cursor() as cursor:
        cursor.execute(merge_sql)
        conn.commit()

    return {
        'statusCode': 200,
        'body': 'Merge successful!'
    }
