from typing import Optional
from decimal import Decimal
import asyncpg

class PointsConfig:
    REFERRAL_SIGNUP = 5
    EMAIL_VERIFIED = 2
    SOCIAL_SHARE_TWITTER = 1
    SOCIAL_SHARE_FACEBOOK = 1
    PREMIUM_UPGRADE = 10
    MILESTONE_BONUS = {
        5: 5,
        10: 15,
        25: 50,
        50: 150,
        100: 500
    }

class PointsManager:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool
        self.config = PointsConfig()
    
    async def award_referral_points(
        self, 
        referrer_id: int, 
        referred_id: int
    ) -> Dict[str, int]:
        """Award points for a referral"""
        points = self.config.REFERRAL_SIGNUP
        
        async with self.db.acquire() as conn:
            async with conn.transaction():
                # Update referrer points
                result = await conn.fetchrow("""
                    UPDATE subscribers 
                    SET 
                        referral_points = referral_points + $1,
                        referral_count = referral_count + 1,
                        total_points = total_points + $1
                    WHERE id = $2
                    RETURNING referral_count
                """, points, referrer_id)
                
                referral_count = result['referral_count']
                
                # Record transaction
                await conn.execute("""
                    INSERT INTO point_transactions (
                        subscriber_id, points, event_type, 
                        event_id, description
                    ) VALUES ($1, $2, $3, $4, $5)
                """, 
                    referrer_id, 
                    points, 
                    'referral',
                    referred_id,
                    f'Referred user ID {referred_id}'
                )
                
                # Check milestone bonus
                bonus = 0
                if referral_count in self.config.MILESTONE_BONUS:
                    bonus = self.config.MILESTONE_BONUS[referral_count]
                    await self._award_bonus_points(
                        conn, 
                        referrer_id, 
                        bonus, 
                        'milestone', 
                        referral_count
                    )
        
        return {
            'points': points,
            'bonus': bonus,
            'total': points + bonus
        }
    
    async def _award_bonus_points(
        self, 
        conn, 
        subscriber_id: int, 
        points: int, 
        event_type: str, 
        event_id: int
    ):
        """Award bonus points (internal)"""
        await conn.execute("""
            UPDATE subscribers 
            SET 
                referral_points = referral_points + $1,
                total_points = total_points + $1
            WHERE id = $2
        """, points, subscriber_id)
        
        await conn.execute("""
            INSERT INTO point_transactions (
                subscriber_id, points, event_type, 
                event_id, description
            ) VALUES ($1, $2, $3, $4, $5)
        """,
            subscriber_id,
            points,
            event_type,
            event_id,
            f'Milestone bonus: {event_id} referrals'
        )
