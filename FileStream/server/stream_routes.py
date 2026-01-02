import time
import math
import logging
import mimetypes
import traceback
import secrets
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from FileStream.bot import multi_clients, work_loads, FileStream
from FileStream.config import Telegram, Server
from FileStream.server.exceptions import FIleNotFound, InvalidHash
from FileStream import utils, StartTime, __version__
from FileStream.utils.render_template import render_page
from FileStream.utils.database import Database
from FileStream.utils.human_readable import humanbytes
from FileStream.utils.file_properties import get_file_info

db = Database(Telegram.DATABASE_URL, Telegram.SESSION_NAME)
routes = web.RouteTableDef()

@routes.get("/status", allow_head=True)
async def root_route_handler(_):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": utils.get_readable_time(time.time() - StartTime),
            "telegram_bot": "@" + FileStream.username,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    )

@routes.get("/watch/{path}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        return web.Response(text=await render_page(path), content_type='text/html')
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass


@routes.get("/dl/{path}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        return await media_streamer(request, path)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        traceback.print_exc()
        logging.critical(e.with_traceback(None))
        logging.debug(traceback.format_exc())
        raise web.HTTPInternalServerError(text=str(e))

# ---------------------------------------------------------------------------------------------------------
# Secure API Endpoint (Refined for Cross-Bot Access)
# ---------------------------------------------------------------------------------------------------------

@routes.post("/api/v1/generate")
async def api_generate_handler(request: web.Request):
    """
    Secure API to generate stream/download links.
    """
    # 1. Security Check
    if not Server.API_SECRET:
        return web.json_response(
            {"status": "error", "message": "API endpoint disabled. API_SECRET not set on server."}, 
            status=503
        )

    api_key = request.headers.get("X-API-KEY")
    if not api_key or not secrets.compare_digest(api_key, Server.API_SECRET):
        return web.json_response(
            {"status": "error", "message": "Unauthorized. Invalid or missing X-API-KEY."}, 
            status=401
        )

    # 2. Parse Data
    try:
        data = await request.json()
    except Exception:
        return web.json_response(
            {"status": "error", "message": "Invalid JSON body."}, 
            status=400
        )

    # 3. Process Request
    try:
        if "_id" in data:
            # Lookup existing file
            db_id = data["_id"]
            file_info = await db.get_file(db_id)
        
        elif "msg_info" in data:
            # Register new file by fetching message (Solving access_hash issue)
            msg_data = data["msg_info"]
            chat_id = msg_data.get("chat_id")
            message_id = msg_data.get("message_id")

            if not chat_id or not message_id:
                return web.json_response({"status": "error", "message": "msg_info requires 'chat_id' and 'message_id'"}, status=400)

            try:
                # 1. FileStreamBot fetches the message from the source channel
                msg = await FileStream.get_messages(chat_id, message_id)
                
                if not msg or not msg.media:
                     return web.json_response({"status": "error", "message": "Message not found or has no media."}, status=404)

                # 2. IMPORTANT: Copy the message to the Bot's Log Channel (FLOG_CHANNEL)
                # This ensures the bot has a permanent reference and valid access hash for itself.
                if not Telegram.FLOG_CHANNEL:
                     return web.json_response({"status": "error", "message": "FLOG_CHANNEL is not set in config."}, status=500)

                copied_msg = await msg.copy(Telegram.FLOG_CHANNEL)

                # 3. Extract clean file info from the COPIED message (which belongs to the bot now)
                f_info = get_file_info(copied_msg)
                
                # 4. Add to DB (db.add_file handles duplication via file_unique_id)
                db_id = await db.add_file(f_info)
                file_info = await db.get_file(db_id)
                
            except Exception as e:
                logging.error(f"API Fetch Error: {e}")
                return web.json_response({"status": "error", "message": f"Error processing file. Ensure Bot is Admin in source channel. details: {str(e)}"}, status=400)
            
        else:
             return web.json_response({"status": "error", "message": "Request must contain '_id' or 'msg_info'."}, status=400)

        # 4. Generate Response
        stream_link = f"{Server.URL}watch/{file_info['_id']}"
        download_link = f"{Server.URL}dl/{file_info['_id']}"
        
        return web.json_response({
            "status": "success",
            "data": {
                "_id": str(file_info['_id']),
                "file_name": file_info['file_name'],
                "file_size": file_info['file_size'],
                "file_size_human": humanbytes(file_info['file_size']),
                "mime_type": file_info['mime_type'],
                "stream_link": stream_link,
                "download_link": download_link
            }
        })

    except FIleNotFound:
        return web.json_response({"status": "error", "message": "File not found."}, status=404)
    except Exception as e:
        traceback.print_exc()
        return web.json_response({"status": "error", "message": str(e)}, status=500)

# ---------------------------------------------------------------------------------------------------------

class_cache = {}

async def media_streamer(request: web.Request, db_id: str):
    range_header = request.headers.get("Range", 0)
    
    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]
    
    if Telegram.MULTI_CLIENT:
        logging.info(f"Client {index} is now serving {request.headers.get('X-FORWARDED-FOR',request.remote)}")

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        logging.debug(f"Using cached ByteStreamer object for client {index}")
    else:
        logging.debug(f"Creating new ByteStreamer object for client {index}")
        tg_connect = utils.ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect
    logging.debug("before calling get_file_properties")
    file_id = await tg_connect.get_file_properties(db_id, multi_clients)
    logging.debug("after calling get_file_properties")
    
    file_size = file_id.file_size

    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    body = tg_connect.yield_file(
        file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )

    mime_type = file_id.mime_type
    file_name = utils.get_name(file_id)
    disposition = "attachment"

    if not mime_type:
        mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"

    # if "video/" in mime_type or "audio/" in mime_type:
    #     disposition = "inline"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": f"{mime_type}",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
        },
    )
