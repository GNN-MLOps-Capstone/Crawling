import boto3
from modules.loading.news_loader import NewsLoader
from modules.loading.keyword_loader import KeywordLoader
from modules.loading.stock_loader import StockLoader


def run_db_loading(targets: list, snapshot_path: str, aws_info: dict, pg_info: dict):
    s3 = boto3.client('s3', aws_access_key_id=aws_info['access_key'], aws_secret_access_key=aws_info['secret_key'],
                      endpoint_url=aws_info['endpoint_url'])

    # Loaders 초기화
    news_loader = NewsLoader(pg_info)
    kw_loader = KeywordLoader(pg_info)
    stock_loader = StockLoader(pg_info)

    # 1. Master Data Sync (Keywords)
    if snapshot_path:
        kw_loader.sync_master_keywords(s3, 'silver', snapshot_path)

    for target in targets:
        print(f"🚀 Loading data for date: {target['date']}")

        # 2. Main News Table
        news_loader.load_filtered_news(s3, 'silver', target['refined'])

        # 3. Mappings
        stock_loader.load_mappings(s3, 'silver', target['stocks'])
        kw_loader.load_mappings(s3, 'silver', target['keywords'])

    return "Done"