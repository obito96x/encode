from pymongo.errors import ServerSelectionTimeoutError
from pymongo.errors import PyMongoError


from bot import asyncio, bot_id
from bot.config import _bot, conf
from bot.startup.before import ffmpegdb, filterdb, pickle, queuedb, rssdb, userdb

from .bot_utils import list_to_str, sync_to_async
from .local_db_utils import save2db_lcl, save2db_lcl2


_filter = {"_id": bot_id}

database = conf.DATABASE_URL


async def save2db(db="queue", retries=3):
    if not database:
        return await sync_to_async(save2db_lcl)
    d = {"queue": _bot.queue, "batches": _bot.batch_queue}
    data = pickle.dumps(d.get(db))
    _update = {db: data}
    while retries:
        try:
            await sync_to_async(
                queuedb.update_one, _filter, {"$set": _update}, upsert=True
            )
            break
        except ServerSelectionTimeoutError as e:
            retries -= 1
            if not retries:
                raise e
            await asyncio.sleep(0.5)


async def save2db2(data: dict | str = False, db: str = None, user_id: int = None):
    if not database:
        if data is False or db == "rss":
            await sync_to_async(save2db_lcl2, db)
        return

    # Save temp users list
    if data is False and db == "t_users":
        tusers = list_to_str(_bot.temp_users)
        data = pickle.dumps(tusers)
        _update = {"t_users": data}
        await sync_to_async(userdb.update_one, _filter, {"$set": _update}, upsert=True)
        return

    # Per-user watermark
    if db == "watermark" and user_id:
        await sync_to_async(
            userdb.update_one,
            {"_id": user_id},
            {"$set": {"watermark": data}},
            upsert=True
        )
        return

    # Pickle for bot-wide ffmpeg/mux/filter/rss
    p_data = pickle.dumps(data)
    _update = {db: p_data}

    if db in ("ffmpeg", "mux_args", "ffmpeg2", "ffmpeg3", "ffmpeg4"):
        await sync_to_async(ffmpegdb.update_one, _filter, {"$set": _update}, upsert=True)
        return

    if db in ("autoname", "cus_rename", "filter"):
        await sync_to_async(filterdb.update_one, _filter, {"$set": _update}, upsert=True)
        return

    if db == "rss":
        await sync_to_async(rssdb.update_one, _filter, {"$set": _update}, upsert=True)
        return


async def set_watermark(user_id: int, url: str):
    try:
        await sync_to_async(
            userdb.update_one,
            {"_id": user_id},
            {"$set": {"watermark": url}},
            upsert=True
        )
    except PyMongoError as e:
        print(f"[SET_WATERMARK_ERROR] {e}")
        raise

async def get_watermark(user_id: int) -> str | None:
    try:
        result = await sync_to_async(userdb.find_one, {"_id": user_id})
        return result.get("watermark") if result else None
    except PyMongoError as e:
        print(f"[GET_WATERMARK_ERROR] {e}")
        return None

async def delete_watermark(user_id: int):
    try:
        await sync_to_async(
            userdb.update_one,
            {"_id": user_id},
            {"$unset": {"watermark": ""}}
        )
    except PyMongoError as e:
        print(f"[DELETE_WATERMARK_ERROR] {e}")