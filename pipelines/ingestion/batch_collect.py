# apscheduler_example.py
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from collect_naver_news import main

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def job():
    """매시간 실행할 작업"""
    logger.info("=" * 50)
    logger.info("뉴스 수집 작업 시작")
    try:
        main()
        logger.info("뉴스 수집 작업 완료")
    except Exception as e:
        logger.error(f"작업 실패: {e}")
    logger.info("=" * 50)


if __name__ == "__main__":
    scheduler = BlockingScheduler()

    # 매시간 정각에 실행 (cron 표현식 사용)
    scheduler.add_job(
        job,
        CronTrigger(minute=0),  # 매시간 0분에 실행
        id='news_collection',
        name='뉴스 수집 작업'
    )

    logger.info("APScheduler 시작")

    # 시작하자마자 한 번 실행
    job()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("스케줄러 종료")
        scheduler.shutdown()
