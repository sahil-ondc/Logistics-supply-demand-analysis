"""
Flask App with MongoDB - Fast Filtered Aggregation
All metrics update correctly based on filters
"""
import logging
import sys
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import h3

from config import Config
from utils.database import get_db_collection, get_statistics, get_filters
from utils.geojson_loader import load_pincode_geojson
from utils.redis_cache import get_cache, set_cache
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)
logger.info("Logger initialized successfully")
app = Flask(__name__)
CORS(app)
pincode_geojson = None

def initialize_app():
    """Initialize MongoDB connection, statistics, and GeoJSON on startup"""
    global pincode_geojson

    logger.info("=" * 80)
    logger.info("Initializing Logistics Supply-Demand Visualization App")

    try:
        logger.info("Connecting to MongoDB...")
        collection = get_db_collection()
        doc_count = collection.count_documents({})

        logger.info(f"MongoDB Connected — DB: {Config.MONGO_DB_NAME} | Collection: {Config.MONGO_COLLECTION_NAME} | Docs: {doc_count:,}")

        if doc_count == 0:
            logger.warning("No data found in MongoDB! Run the data ingestion script first: python scripts/ingest_data.py")

        stats = get_statistics()
        logger.info(f"Quick Stats Cached: Orders={stats['total_orders']:,}, Success Rate={stats['success_rate']}%, Unique Restaurants={stats['total_restaurants']:,}")

        indexes = collection.index_information()
        logger.info(f"Indexes configured: {len(indexes)}")

        logger.info("Loading pincode GeoJSON boundaries...")
        pincode_geojson = load_pincode_geojson()
        if pincode_geojson:
            logger.info(f"Loaded {len(pincode_geojson['features'])} pincode boundaries.")
        else:
            logger.warning("Failed to load GeoJSON — file missing or unreadable.")
        
        logger.info("Warming cache for default filters (All, All)...")
        Thread(target=lambda: get_hexagons_with_filters('All', 'All')).start()
        Thread(target=lambda: get_supply_points_with_filters('All', 'All')).start()
        Thread(target=lambda: get_statistics('All', 'All')).start()

        logger.info("Initialization complete (cache warming started in background).")


        logger.info("Initialization complete")

    except Exception as e:
        logger.exception(f"Initialization failed: {e}")

def get_hexagons_with_filters(logistics_player='All', hour_bin='All', limit=None):
    """
    Get hexagons WITH FILTERED METRICS using MongoDB aggregation
    This correctly shows metrics for the selected filters
    """
    if limit is None:
        limit = Config.DEFAULT_HEXAGON_LIMIT

    if isinstance(logistics_player, str) and logistics_player != 'All':
        logistics_player = [logistics_player]
    if isinstance(hour_bin, str) and hour_bin != 'All':
        hour_bin = [hour_bin]

    lp_key = ",".join(logistics_player) if isinstance(logistics_player, list) else "All"
    hb_key = ",".join(hour_bin) if isinstance(hour_bin, list) else "All"
    cache_key = f"hexagons:{lp_key}:{hb_key}:{limit}"

    cached = get_cache(cache_key)
    if cached:
        logger.info(f"✅ Cache hit for key: {cache_key}")
        return cached
    else:
        logger.info(f"❌ Cache miss for key: {cache_key} — querying MongoDB")
    
    collection = get_db_collection()
    pipeline = []
    
    # Stage 1: Filter by player and hour
    match_conditions = {}
    if isinstance(logistics_player, list) and 'All' not in logistics_player:
        match_conditions['logistics_player'] = {'$in': logistics_player}
    if isinstance(hour_bin, list) and 'All' not in hour_bin:
        match_conditions['hour_bin'] = {'$in': hour_bin}
    
    if match_conditions:
        pipeline.append({'$match': match_conditions})
        logger.info(f"Applying match conditions: {match_conditions}")
    
    # Stage 2: Group by H3 index with filtered metrics
    pipeline.extend([
        {
            '$group': {
                '_id': f'$h3_res_{Config.DEFAULT_H3_RESOLUTION}',
                'total_orders': {'$sum': 1},
                'successful_orders': {
                    '$sum': {'$cond': [{'$eq': ['$order_status', 'success']}, 1, 0]}
                },
                'failed_orders': {
                    '$sum': {'$cond': [{'$ne': ['$order_status', 'success']}, 1, 0]}
                },
                'avg_lat': {'$avg': '$pickup_lat'},
                'avg_lon': {'$avg': '$pickup_lon'},
                'unique_locations': {
                    '$addToSet': {
                        '$concat': [
                            {'$toString': '$pickup_lat'},
                            ',',
                            {'$toString': '$pickup_lon'}
                        ]
                    }
                },
                'hour_bins': {'$addToSet': '$hour_bin'},
                'logistics_players': {'$addToSet': '$logistics_player'}
            }
        },
        {
            '$project': {
                'h3_index': '$_id',
                'total_orders': 1,
                'successful_orders': 1,
                'failed_orders': 1,
                'success_rate': {
                    '$multiply': [
                        {'$divide': ['$successful_orders', '$total_orders']},
                        100
                    ]
                },
                'unique_restaurants': {'$size': '$unique_locations'},
                'center_lat': '$avg_lat',
                'center_lon': '$avg_lon',
                'hour_bins': 1,
                'logistics_players': 1
            }
        },
        {'$sort': {'total_orders': -1}},
        {'$limit': limit}
    ])
    
    results = list(collection.aggregate(pipeline, allowDiskUse=True))
    
    # Convert to GeoJSON
    features = []
    for result in results:
        try:
            h3_index = result['h3_index']
            boundary = h3.cell_to_boundary(h3_index)
            boundary_coords = [[coord[1], coord[0]] for coord in boundary]
            
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [boundary_coords]
                },
                'properties': {
                    'h3_index': h3_index,
                    'total_orders': result['total_orders'],
                    'success_orders': result['successful_orders'],
                    'fail_orders': result['failed_orders'],
                    'success_rate': round(result['success_rate'], 2),
                    'center_lat': round(result['center_lat'], 6),
                    'center_lng': round(result['center_lon'], 6),
                    'unique_restaurants': result['unique_restaurants'],
                    'hour_bins': ','.join(sorted(result.get('hour_bins', []))),
                    'logistics_players': ','.join([str(p).split('/')[-1] for p in result.get('logistics_players', [])])
                }
            })
        except Exception as e:
            logger.warning(f"Error processing hexagon: {e}")
            continue
    
    geojson = {'type': 'FeatureCollection', 'features': features}
    set_cache(cache_key, geojson, Config.CACHE_EXPIRY_SECONDS)
    logger.info(f"Cached result for key: {cache_key}")

    return geojson


def get_supply_points_with_filters(logistics_player='All', hour_bin='All', limit=None):
    """Get supply points matching the current filters"""
    if limit is None:
        limit = Config.DEFAULT_SUPPLY_POINT_LIMIT

    if isinstance(logistics_player, str) and logistics_player != 'All':
        logistics_player = [logistics_player]
    if isinstance(hour_bin, str) and hour_bin != 'All':
        hour_bin = [hour_bin]

    lp_key = ",".join(logistics_player) if isinstance(logistics_player, list) else "All"
    hb_key = ",".join(hour_bin) if isinstance(hour_bin, list) else "All"
    cache_key = f"supply_points:{lp_key}:{hb_key}:{limit}"

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
    
    pipeline.extend([
        {
            '$group': {
                '_id': {
                    'lat': '$pickup_lat',
                    'lon': '$pickup_lon'
                }
            }
        },
        {'$limit': limit},
        {
            '$project': {
                '_id': 0,
                'lat': '$_id.lat',
                'lon': '$_id.lon'
            }
        }
    ])
    
    results = list(collection.aggregate(pipeline))
    supply_points = [[r['lat'], r['lon']] for r in results]

    set_cache(cache_key, supply_points, Config.CACHE_EXPIRY_SECONDS)
    logger.info(f"Cached result for key: {cache_key}")

    return supply_points
@app.context_processor
def inject_base_vars():
    return dict(base_path=Config.BASE_PATH, base_url=Config.BASE_URL)

initialize_app()

@app.route('/')
def index():
    """Main visualization page"""
    
    stats = get_statistics()
    logistics_players, hour_bins = get_filters()
    initial_hexagons = get_hexagons_with_filters()
    initial_supply_points = get_supply_points_with_filters()
    
    return render_template(
        'index.html',
        initial_hexagons=initial_hexagons,
        initial_supply_points=initial_supply_points,
        total_orders=f"{stats['total_orders']:,}",
        total_restaurants=f"{stats['total_restaurants']:,}",
        success_rate=f"{stats['success_rate']:.1f}",
        hexagon_count=f"{len(initial_hexagons['features']):,}",
        pincode_data=pincode_geojson if pincode_geojson else {},
        logistics_players=logistics_players,
        hour_bins=hour_bins
    )

@app.route(f"{Config.BASE_PATH}/filter_hexagons", methods=['POST'])
def filter_hexagons():
    """API endpoint to filter hexagons - Returns FILTERED metrics"""
    try:
        data = request.get_json()
        logistics_player = data.get('logistics_player', 'All')
        hour_bin = data.get('hour_bin', 'All')

        if isinstance(logistics_player, str):
            logistics_player = [logistics_player]
        if isinstance(hour_bin, str):
            hour_bin = [hour_bin]
        
        # Get hexagons with FILTERED metrics
        hexagons = get_hexagons_with_filters(logistics_player, hour_bin)
        
        # Get supply points matching filters
        supply_points = get_supply_points_with_filters(logistics_player, hour_bin)
        
        # Get statistics matching filters
        stats = get_statistics(logistics_player, hour_bin)
        
        return jsonify({
            'hexagons': hexagons,
            'supply_points': supply_points,
            'stats': stats
        })
    except Exception as e:
        logger.exception("Error in /filter_hexagons")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        collection = get_db_collection()
        doc_count = collection.count_documents({})
        return jsonify({
            'status': 'healthy',
            'database': Config.MONGO_DB_NAME,
            'collection': Config.MONGO_COLLECTION_NAME,
            'total_documents': doc_count
        })
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host=Config.FLASK_HOST, port=Config.FLASK_PORT, debug=(Config.FLASK_ENV == 'development'))