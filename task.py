from celery import Celery
from celery.schedules import crontab
import asyncio

celery = Celery(
    'openlist',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# Schedule periodic tasks
celery.conf.beat_schedule = {
    'update-all-leaderboards': {
        'task': 'tasks.update_all_leaderboards',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
    },
    'check-giveaway-deadlines': {
        'task': 'tasks.check_giveaway_deadlines',
        'schedule': crontab(minute=0, hour='*'),  # Every hour
    },
}

@celery.task
def update_all_leaderboards():
    """Update all active leaderboards"""
    async def _update():
        db_pool = await get_db_pool()
        redis_client = get_redis_client()
        
        manager = LeaderboardManager(db_pool, redis_client)
        
        # Get all active waitlists
        waitlists = await db_pool.fetch(
            "SELECT id FROM waitlists WHERE status = 'active'"
        )
        
        for waitlist in waitlists:
            await manager.update_leaderboard(waitlist['id'])
    
    asyncio.run(_update())

@celery.task
def check_giveaway_deadlines():
    """Check and end expired giveaways"""
    async def _check():
        db_pool = await get_db_pool()
        manager = GiveawayManager(db_pool)
        
        # Get expired giveaways
        expired = await db_pool.fetch("""
            SELECT id FROM giveaways
            WHERE status = 'active'
                AND end_date <= NOW()
        """)
        
        for giveaway in expired:
            await manager.select_winners(giveaway['id'])
    
    asyncio.run(_check())

@celery.task
def calculate_daily_analytics(waitlist_id: int):
    """Calculate analytics for a waitlist"""
    async def _calculate():
        db_pool = await get_db_pool()
        analytics = AnalyticsEngine(db_pool)
        
        # Calculate all metrics
        viral_coef = await analytics.calculate_viral_coefficient(waitlist_id)
        growth = await analytics.get_growth_rate(waitlist_id)
        percentiles = await analytics.calculate_percentiles(waitlist_id)
        
        # Save to database
        await db_pool.execute("""
            INSERT INTO daily_analytics (
                waitlist_id, date, viral_coefficient,
                growth_rate, percentiles
            ) VALUES ($1, CURRENT_DATE, $2, $3, $4)
        """, waitlist_id, viral_coef, growth['avg_growth_rate'], percentiles)
    
    asyncio.run(_calculate())
