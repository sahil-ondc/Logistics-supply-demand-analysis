"""
Database utilities for MongoDB operations
"""
import logging
from .redis_cache import get_cache, set_cache
from pymongo import MongoClient
from config import Config

_client = None
_db = None
_collection = None
logger = logging.getLogger(__name__)
def get_db_collection():
    """Get MongoDB collection (singleton pattern)"""
    global _client, _db, _collection
    
    if _collection is None:
        _client = MongoClient(Config.MONGO_URI)
        _db = _client[Config.MONGO_DB_NAME]
        _collection = _db[Config.MONGO_COLLECTION_NAME]
    
    return _collection

def get_statistics(logistics_player='All', hour_bin='All'):
    """Get statistics with filters"""

    if isinstance(logistics_player, str) and logistics_player != 'All':
        logistics_player = [logistics_player]
    if isinstance(hour_bin, str) and hour_bin != 'All':
        hour_bin = [hour_bin]

    lp_key = ",".join(logistics_player) if isinstance(logistics_player, list) else "All"
    hb_key = ",".join(hour_bin) if isinstance(hour_bin, list) else "All"
    cache_key = f"stats:{lp_key}:{hb_key}"

    # 1️⃣ Try fetching from cache first
    cached = get_cache(cache_key)
    if cached:
        logger.info(f"✅ Cache hit for key: {cache_key}")
        return cached
    else:
        logger.info(f"❌ Cache miss for key: {cache_key} — querying MongoDB")

    
    collection = get_db_collection()
    
    pipeline = []
    
    match_conditions = {}
    if isinstance(logistics_player, list) and 'All' not in logistics_player:
        match_conditions['logistics_player'] = {'$in': logistics_player}
    if isinstance(hour_bin, list) and 'All' not in hour_bin:
        match_conditions['hour_bin'] = {'$in': hour_bin}
    
    if match_conditions:
        pipeline.append({'$match': match_conditions})
        logger.info(f"Applying match conditions: {match_conditions}")
    else:
        logger.info("No match conditions applied — querying all data")
    
    pipeline.append({
        '$group': {
            '_id': None,
            'total_orders': {'$sum': 1},
            'successful_orders': {
                '$sum': {'$cond': [{'$eq': ['$order_status', 'success']}, 1, 0]}
            },
            'unique_locations': {
                '$addToSet': {
                    '$concat': [
                        {'$toString': '$pickup_lat'},
                        ',',
                        {'$toString': '$pickup_lon'}
                    ]
                }
            }
        }
    })
    
    pipeline.append({
        '$project': {
            'total_orders': 1,
            'successful_orders': 1,
            'success_rate': {
                '$multiply': [
                    {'$divide': ['$successful_orders', '$total_orders']},
                    100
                ]
            },
            'total_restaurants': {'$size': '$unique_locations'}
        }
    })
    
    results = list(collection.aggregate(pipeline))
    
    if results:
        result = results[0]
        final = {
            'total_orders': result['total_orders'],
            'successful_orders': result['successful_orders'],
            'success_rate': round(result['success_rate'], 1),
            'total_restaurants': result['total_restaurants']
        }
    else:
        final = {'total_orders': 0, 'successful_orders': 0, 'success_rate': 0, 'total_restaurants': 0}
    
    set_cache(cache_key, final, Config.CACHE_EXPIRY_SECONDS)
    logger.info(f"Cached result for key: {cache_key}")
    
    return final

def get_filters():
    """Get unique filter values"""
    cache_key = "filters:logistics_player_hour_bin"
    cached = get_cache(cache_key)
    if cached:
        logger.info(f"✅ Cache hit for filters")
        return cached
    else:
        logger.info("❌ Cache miss for filters — fetching from MongoDB")

    collection = get_db_collection()
    
    logistics_players = collection.distinct(
        'logistics_player',
        {'logistics_player': {'$nin': [None, '', 'unknown']}}
    )

    logistics_players = sorted({
        str(p).strip() for p in logistics_players if isinstance(p, str) and p.strip()
    })

    hour_bins = collection.distinct('hour_bin')
    hour_bins = sorted({
        str(h).strip() for h in hour_bins if isinstance(h, str) and h.strip()
    })

    filters = {
        "logistics_players": logistics_players,
        "hour_bins": hour_bins
    }

    set_cache(cache_key, filters, Config.CACHE_EXPIRY_SECONDS * 10)
    logger.info("✅ Cached filters successfully")

    return filters["logistics_players"], filters["hour_bins"]