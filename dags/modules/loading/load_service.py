import boto3
from modules.loading.news_loader import NewsLoader
from modules.loading.keyword_loader import KeywordLoader
from modules.loading.stock_loader import StockLoader


def run_db_loading(targets: list, snapshot_path: str, aws_info: dict, pg_info: dict):
    """
    [Service Layer]
    DB 적재를 총괄합니다.
    Gemini 분석 결과(Summary, Sentiment) 반영을 위해 로직이 확장되었습니다.
    """
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

        # 2. Main News Table (기본 정보 + 임베딩)
        # news_service.py가 만든 refined_news 적재
        news_loader.load_filtered_news(s3, 'silver', target['refined'])

        # 3. [NEW] Update Analysis Results (Summary & Sentiment)
        # Gemini가 생성한 analyzed_news를 이용해 기존 뉴스 테이블 업데이트
        if target.get('analysis'):
            # NewsLoader에 update_analysis_info 메서드가 구현되어 있다고 가정
            # (UPDATE news SET summary=..., sentiment=... WHERE news_id=...)
            try:
                news_loader.update_analysis_info(s3, 'silver', target['analysis'])
                print(f"✅ Updated Analysis (Summary/Sentiment) for {target['date']}")
            except AttributeError:
                print("⚠️ NewsLoader does not support analysis update yet.")

        # 4. Mappings (Stocks & Keywords)
        if target.get('stocks'):
            stock_loader.load_mappings(s3, 'silver', target['stocks'])

        if target.get('keywords'):
            kw_loader.load_mappings(s3, 'silver', target['keywords'])

    return "Done"


def run_incremental_db_loading(target: dict, aws_info: dict, pg_info: dict, snapshot_path: str = None):
    label = target.get('window_label') or target.get('date') or 'incremental'
    return run_db_loading(
        targets=[{
            "date": label,
            "refined": target.get("refined"),
            "stocks": target.get("stocks"),
            "keywords": target.get("keywords"),
            "analysis": target.get("analysis"),
        }],
        snapshot_path=snapshot_path,
        aws_info=aws_info,
        pg_info=pg_info,
    )
