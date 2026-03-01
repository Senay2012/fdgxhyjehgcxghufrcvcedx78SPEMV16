"""
TELEGRAM РАССЫЛКА PRO v16.0
- Без авторизации (доступен всем)
- Рассылка активным участникам в ЛС
- База отправленных: никогда не пишет дважды одному человеку
- Проверка открытости ЛС перед отправкой
- 👁 Мониторинг в реальном времени (пишет сразу кто написал в чат)
- 📤 Массовая рассылка по готовой базе (файл .txt или текст списком)

pip install telethon aiogram
python bot_sender.py
"""
import asyncio
import json
import random
import math
import traceback
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup as IKM,
    InlineKeyboardButton as IKB,
    ReplyKeyboardMarkup as RKM,
    KeyboardButton as KB,
    FSInputFile,
    BotCommand,
    ReplyKeyboardRemove,
)
from telethon import TelegramClient, events
from telethon.tl.types import (
    User,
    Channel,
    Chat,
    InputMessagesFilterPinned,
    PeerChannel,
    PeerChat,
    PeerUser,
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError,
    FloodWaitError,
    UserPrivacyRestrictedError,
    UserIsBlockedError,
)

# ══════════════ КОНФИГ ══════════════
BOT_TOKEN = "8578105208:AAEpmph7onmU_VX8yJ1KSgU3-Pl2TI0lpFU"
DATA = Path("bot_data")
DATA.mkdir(exist_ok=True)

F = {k: DATA / f for k, f in {
    "acc":      "accounts.json",
    "msg":      "messages.json",
    "set":      "settings.json",
    "stat":     "stats.json",
    "tgt":      "targets.json",
    "tpl":      "templates.json",
    "log":      "log.txt",
    "dm":       "dm_settings.json",
    "dmst":     "dm_stats.json",
    "sent_dm":  "dm_sent.json",
    "notify":   "notify_users.json",
    "monitor":  "monitor.json",     # настройки живого мониторинга
    "mass":     "mass_dm.json",     # настройки массовой рассылки по базе
    "mass_users": "mass_users.txt", # загруженные юзеры
}.items()}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
clients: dict = {}
pin_cache: dict = {}

# ══════════════ IO ══════════════
def ld(p, d=None):
    try:
        if p.exists():
            return json.loads(p.read_text("utf-8"))
    except Exception:
        pass
    return d if d is not None else {}

def sv(p, d):
    p.write_text(json.dumps(d, ensure_ascii=False, indent=2), "utf-8")

# Загрузка данных
accs = ld(F["acc"], [])
if not accs:
    accs.append({
        "phone": "+79497221631",
        "api_id": "32519480",
        "api_hash": "21f4fd7a1294cab8d5cc439c279ca624",
        "session": "s_79497221631",
        "name": "",
        "on": True,
    })
    sv(F["acc"], accs)

msgs    = ld(F["msg"], [])
targets = ld(F["tgt"], {})
tpl     = ld(F["tpl"], {})

sett = ld(F["set"], {
    "interval_sec": 900,
    "msg_idx": -1,
    "delay": 0.3,
    "auto_del": 0,
    "shuffle": False,
    "emoji": False,
    "schedule": [],
})
# Миграция старого ключа interval → interval_sec
if "interval" in sett and "interval_sec" not in sett:
    sett["interval_sec"] = sett.pop("interval") * 60
    sv(F["set"], sett)
if "interval_sec" not in sett:
    sett["interval_sec"] = 900

stat = ld(F["stat"], {})
for _k in ("sent", "err", "skip", "runs"):
    if _k not in stat:
        stat[_k] = 0
if "hist" not in stat:
    stat["hist"] = []

# Настройки модуля «Активные участники»
dm_cfg = ld(F["dm"], {
    "text": "",           # текст для рассылки
    "chats": [],          # список chat_id для анализа
    "top_n": 10,          # сколько самых активных брать
    "scan_limit": 500,    # сколько последних сообщений анализировать
    "delay": 2.0,         # задержка между ЛС (сек)
    "running": False,
})

dm_stat = ld(F["dmst"], {"sent": 0, "fail": 0, "last_run": "", "log": []})

# ── База отправленных ЛС ──────────────────────────────────────────────────────
_sent_dm_raw = ld(F["sent_dm"], {})
dm_sent: dict = _sent_dm_raw   # ключ — str(user_id)

# ── Настройки живого мониторинга ──────────────────────────────────────────────
monitor_cfg = ld(F["monitor"], {
    "active":   False,      # включён ли мониторинг прямо сейчас
    "chats":    [],         # chat_id которые слушаем (str)
    "text":     "",         # текст для ЛС (если пусто — берётся из dm_cfg["text"])
    "use_dm_sent": True,    # использовать общую базу dm_sent (не писать тем кому уже писали)
    "delay":    1.5,        # задержка перед отправкой ЛС после события (сек)
})
# Счётчик мониторинга (в памяти, не сохраняется)
monitor_stat = {"sent": 0, "skip": 0, "fail": 0, "events": 0}

# ── Массовая рассылка по загруженной базе ────────────────────────────────────
mass_cfg = ld(F["mass"], {
    "text": "",           # текст для рассылки
    "users": [],         # список username/id загруженных (кешируется)
    "delay": 2.0,        # задержка между ЛС
    "use_dm_sent": True, # использовать общую базу
    "running": False,
})
mass_stat = {"sent": 0, "fail": 0, "skip": 0, "last_run": ""}

# ── Пользователи для уведомлений (сохраняются между перезапусками) ────────────
_notify_raw = ld(F["notify"], [])
notify_users: list = _notify_raw   # список str(user_id)

PER_PAGE = 8
tgt_list = list(targets.keys())

# Состояние бота (глобальное)
S = {
    "run": False, "pause": False, "task": None, "cnt": 0, "next": None,
    "force": False, "wait": {},  "tmp": {}, "mtype": None, "mupl": {},
    "page": 0, "ft": "all", "ft_acc": "all",
    "dm_task": None,
    "monitor_task": None,   # задача живого мониторинга
    "mass_task": None,      # задача массовой рассылки
}

# ══════════════ УТИЛИТЫ ══════════════
def log(t: str):
    line = f"[{datetime.now():%H:%M:%S}] {t}"
    print(line)
    try:
        with open(F["log"], "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def rebuild_list():
    global tgt_list
    tgt_list = list(targets.keys())

def get_uid(m) -> str:
    return str(getattr(getattr(m, "from_user", None), "id", 0))

def get_wait(m) -> str:
    return S["wait"].get(get_uid(m), "")

def set_wait(m, v: str):
    S["wait"][get_uid(m)] = v

def fmt_iv(sec: int) -> str:
    if sec < 60:
        return f"{sec} сек"
    m2, s2 = divmod(sec, 60)
    if m2 < 60:
        return f"{m2} мин" if s2 == 0 else f"{m2}м {s2}с"
    return f"{sec // 3600}ч {(sec % 3600) // 60}м"

def acc_short(ph: str) -> str:
    for a in accs:
        if a["phone"] == ph:
            return a.get("name", "")[:15] or ph[-4:]
    return ph[-4:]

# ══════════════ АККАУНТЫ ══════════════
async def conn(a: dict) -> bool:
    """Подключает один аккаунт Telethon."""
    ph = a["phone"]
    if ph in clients and clients[ph].is_connected():
        return True
    try:
        c = TelegramClient(
            str(DATA / a["session"]),
            int(a["api_id"]),
            a["api_hash"],
        )
        await c.connect()
        if not await c.is_user_authorized():
            return False
        clients[ph] = c
        pin_cache[ph] = {}
        me = await c.get_me()
        a["name"] = f"{me.first_name or ''} @{me.username or '?'}"
        sv(F["acc"], accs)
        return True
    except Exception as ex:
        log(f"conn err {a['phone']}: {ex}")
        return False

async def conn_all() -> int:
    ok = 0
    for a in accs:
        if a.get("on", True) and await conn(a):
            ok += 1
    return ok

def active() -> list:
    return [a for a in accs if a.get("on", True) and a["phone"] in clients]

# ══════════════ СКАН ДИАЛОГОВ ══════════════
async def scan_all(progress_cb=None):
    found = 0
    total_scanned = 0
    for a in active():
        c = clients[a["phone"]]
        me = await c.get_me()
        for folder_id in (0, 1):
            try:
                dlgs = await c.get_dialogs(limit=None, folder=folder_id)
            except Exception:
                continue
            is_arch = folder_id == 1
            for d in dlgs:
                e = d.entity
                eid = getattr(e, "id", None)
                if not eid:
                    continue
                if isinstance(e, User) and e.id == me.id:
                    continue
                if eid == 777000:
                    continue
                if isinstance(e, User) and (e.first_name or "") == "Telegram":
                    continue
                total_scanned += 1
                sid = str(eid)
                if isinstance(e, Channel):
                    tp = "📢кан" if getattr(e, "broadcast", False) else "👥суп"
                    p = "ch"
                elif isinstance(e, Chat):
                    tp = "👥гр"
                    p = "gr"
                elif isinstance(e, User) and getattr(e, "bot", False):
                    tp = "🤖бот"
                    p = "u"
                elif isinstance(e, User):
                    tp = "👤лс"
                    p = "u"
                else:
                    tp = "❓"
                    p = "u"
                if sid not in targets:
                    targets[sid] = {
                        "n": d.name or "?", "t": tp, "p": p,
                        "mi": -1, "on": False, "ph": a["phone"],
                        "pin": False, "arch": is_arch,
                    }
                    found += 1
                else:
                    targets[sid].update({
                        "n": d.name or "?", "t": tp, "p": p,
                        "ph": a["phone"], "arch": is_arch,
                    })
    sv(F["tgt"], targets)
    rebuild_list()
    return found, len(targets), total_scanned

async def scan_pins() -> int:
    cnt = 0
    for a in active():
        c = clients[a["phone"]]
        cache = pin_cache.setdefault(a["phone"], {})
        for sid, tgt in targets.items():
            if tgt.get("ph") != a["phone"]:
                continue
            eid = int(sid)
            try:
                if tgt["p"] == "ch":
                    entity = await c.get_entity(PeerChannel(eid))
                elif tgt["p"] == "gr":
                    entity = await c.get_entity(PeerChat(eid))
                else:
                    continue
                pin_id = await get_pin(c, entity, cache)
                tgt["pin"] = bool(pin_id)
                if pin_id:
                    cnt += 1
            except Exception:
                pass
            await asyncio.sleep(0.1)
    sv(F["tgt"], targets)
    return cnt

# ══════════════ RESOLVE ══════════════
async def resolve(client, sid: str, tgt: dict):
    eid = int(sid)
    p = tgt.get("p", "u")
    for attempt in (
        (PeerChannel(eid) if p == "ch" else PeerChat(eid) if p == "gr" else PeerUser(eid)),
        eid,
    ):
        try:
            return await client.get_entity(attempt)
        except Exception:
            pass
    return None

# ══════════════ ЗАКРЕПЫ ══════════════
async def get_pin(client, entity, cache: dict):
    eid = getattr(entity, "id", None)
    if eid and eid in cache:
        return cache[eid]
    # Способ 1: через фильтр
    try:
        r = await client.get_messages(entity, filter=InputMessagesFilterPinned(), limit=1)
        if r and r[0]:
            if eid:
                cache[eid] = r[0].id
            return r[0].id
    except Exception:
        pass
    # Способ 2: через full info
    try:
        if isinstance(entity, Channel):
            f = await client(GetFullChannelRequest(entity))
            pid = f.full_chat.pinned_msg_id
        elif isinstance(entity, Chat):
            f = await client(GetFullChatRequest(entity.id))
            pid = f.full_chat.pinned_msg_id
        else:
            pid = None
        if pid:
            if eid:
                cache[eid] = pid
            return pid
    except Exception:
        pass
    return None

# ══════════════ ОТПРАВКА ОДНОГО СООБЩЕНИЯ ══════════════
async def send1(client, entity, text: str, cache: dict, media=None, mtype=None) -> str:
    try:
        if media and mtype:
            await client.send_file(entity, file=media, caption=text)
        else:
            sent_msg = await client.send_message(entity, text)
            ad = sett.get("auto_del", 0)
            if ad > 0:
                asyncio.create_task(_adel(client, entity, sent_msg.id, ad))
        d = sett.get("delay", 0.3)
        if d > 0:
            await asyncio.sleep(d)
        return "ok"
    except FloodWaitError as e:
        log(f"Flood {e.seconds}s")
        await asyncio.sleep(min(e.seconds, 60))
        return "ok"
    except Exception as e:
        return f"err:{e}"

async def _adel(c, e, mid: int, s: int):
    await asyncio.sleep(s)
    try:
        await c.delete_messages(e, [mid])
    except Exception:
        pass

# ══════════════ ВЫБОР СООБЩЕНИЯ ══════════════
def chat_msg(tgt: dict):
    """Возвращает (text, mtype, media) для конкретного чата."""
    if not msgs:
        return None, None, None
    mi = tgt.get("mi", -1)
    if 0 <= mi < len(msgs):
        text = msgs[mi]["text"]
    elif 0 <= sett.get("msg_idx", -1) < len(msgs):
        text = msgs[sett["msg_idx"]]["text"]
    else:
        text = msgs[0]["text"]
    if sett.get("emoji"):
        emojis = "🔥⚡💎🚀✨💫🌟💥🎯💰👑🏆⭐🎉💪"
        text = f"{random.choice(emojis)} {text}"
    return text, S["mtype"], None

# ══════════════ РАССЫЛКА ══════════════
async def broadcast():
    enabled = [(sid, targets[sid]) for sid in targets if targets[sid].get("on")]
    if sett.get("shuffle"):
        random.shuffle(enabled)
    if not enabled:
        return 0, 0, 0, []
    ts = tsk = te = 0
    errs = []
    for sid, tgt in enabled:
        text, mtype, _ = chat_msg(tgt)
        if not text:
            tsk += 1
            continue
        ph = tgt.get("ph")
        if not ph or ph not in clients:
            aa = active()
            if not aa:
                te += 1
                continue
            ph = aa[0]["phone"]
        acc_obj = next((a for a in accs if a["phone"] == ph), None)
        if acc_obj and not acc_obj.get("on", True):
            tsk += 1
            continue
        c = clients[ph]
        cache = pin_cache.setdefault(ph, {})
        media = S["mupl"].get(ph)
        try:
            entity = await resolve(c, sid, tgt)
            if not entity:
                te += 1
                if len(errs) < 8:
                    errs.append(f"❌{tgt['n'][:15]}: не найден")
                continue
            r = await send1(c, entity, text, cache, media, mtype)
            if r == "ok":
                ts += 1
            else:
                te += 1
                if len(errs) < 8:
                    errs.append(f"❌{tgt['n'][:15]}: {r[:40]}")
        except Exception as e:
            te += 1
            if len(errs) < 8:
                errs.append(f"❌{tgt['n'][:15]}: {str(e)[:40]}")
    return ts, tsk, te, errs

# ══════════════ ЦИКЛ РАССЫЛКИ ══════════════
async def loop():
    while S["run"]:
        try:
            while S["pause"] and S["run"]:
                await asyncio.sleep(1)
            if not S["run"]:
                break
            sc = sett.get("schedule", [])
            if sc and datetime.now().hour not in sc:
                await asyncio.sleep(30)
                continue
            log("Рассылка...")
            s, sk, e, errs = await broadcast()
            if len(msgs) > 1 and sett.get("rotate_on", False):
                sett["msg_idx"] = (sett.get("msg_idx", 0) + 1) % len(msgs)
                sv(F["set"], sett)
            S["cnt"] += 1
            stat["sent"] += s
            stat["err"] += e
            stat["skip"] += sk
            stat["runs"] += 1
            stat["hist"].append({
                "d": f"{datetime.now():%m-%d %H:%M}",
                "s": s, "e": e, "sk": sk,
            })
            if len(stat["hist"]) > 100:
                stat["hist"] = stat["hist"][-100:]
            sv(F["stat"], stat)
            log(f"#{S['cnt']}: ✅{s} ⏭{sk} ❌{e}")
            isec = sett.get("interval_sec", 300)
            S["next"] = datetime.now() + timedelta(seconds=isec)
            S["force"] = False
            report = (
                f"📤 <b>Рассылка #{S['cnt']}</b>\n"
                f"✅ Отправлено: {s}\n⏭ Без закрепа: {sk}\n❌ Ошибок: {e}"
            )
            if errs:
                report += "\n\n" + "\n".join(errs[:5])
            # Уведомления всем кто написал /start
            for uid_str in list(notify_users):
                try:
                    await bot.send_message(int(uid_str), report, parse_mode="HTML")
                except Exception:
                    pass
            if not S["run"]:
                break
            timer = f"⏳ Следующая через <b>{fmt_iv(isec)}</b> в {S['next']:%H:%M:%S}"
            for uid_str in list(notify_users):
                try:
                    await bot.send_message(int(uid_str), timer, parse_mode="HTML")
                except Exception:
                    pass
            for _ in range(isec):
                if not S["run"]:
                    return
                if S["force"]:
                    S["force"] = False
                    break
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            log("Цикл отменён")
            return
        except Exception as ex:
            log(f"ERR loop: {traceback.format_exc()}")
            for uid_str in list(notify_users):
                try:
                    await bot.send_message(
                        int(uid_str),
                        f"⚠️ <code>{str(ex)[:200]}</code>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            await asyncio.sleep(10)

# ══════════════════════════════════════════════════
# ███  МОДУЛЬ: АКТИВНЫЕ УЧАСТНИКИ → ЛС  ███
# ══════════════════════════════════════════════════

async def get_active_users(client, chat_id: int, limit: int = 500) -> list:
    """
    Анализирует последние `limit` сообщений в чате.
    Возвращает список {'user_id', 'username', 'name', 'count'} — отсортировано по убыванию.
    Боты, удалённые и анонимные — исключаются.
    """
    try:
        entity = await client.get_entity(chat_id)
    except Exception as e:
        log(f"get_active_users get_entity err {chat_id}: {e}")
        return []

    counter = Counter()
    user_info: dict = {}

    try:
        async for msg in client.iter_messages(entity, limit=limit):
            sender = msg.sender
            if sender is None:
                continue
            if not isinstance(sender, User):
                continue
            if getattr(sender, "bot", False):
                continue
            if getattr(sender, "deleted", False):
                continue
            uid = sender.id
            counter[uid] += 1
            if uid not in user_info:
                name_parts = [sender.first_name or "", sender.last_name or ""]
                user_info[uid] = {
                    "user_id": uid,
                    "username": sender.username or "",
                    "name": " ".join(p for p in name_parts if p).strip() or f"id{uid}",
                    "count": 0,
                }
    except Exception as e:
        log(f"iter_messages err {chat_id}: {e}")

    result = []
    for uid, cnt in counter.most_common():
        if uid in user_info:
            user_info[uid]["count"] = cnt
            result.append(user_info[uid])
    return result


async def check_dm_open(client, user_id: int) -> bool:
    """
    Возвращает True если ЛС с пользователем УЖЕ ОТКРЫТ на аккаунте
    (то есть мы уже писали ему или он писал нам — диалог существует).
    Возвращает False если диалога нет совсем — значит раньше не общались,
    человека пропускаем согласно задаче.

    Дополнительно фильтрует: боты и удалённые аккаунты → False.
    """
    try:
        entity = await client.get_entity(PeerUser(user_id))
        # Боты и удалённые — сразу пропускаем
        if getattr(entity, "bot", False):
            return False
        if getattr(entity, "deleted", False):
            return False
    except Exception:
        return False

    # Ищем диалог с этим пользователем в списке диалогов аккаунта
    # get_dialogs возвращает все открытые чаты — если там есть этот user_id, ЛС открыт
    try:
        async for dialog in client.iter_dialogs():
            e = dialog.entity
            if isinstance(e, User) and e.id == user_id:
                return True   # ЛС существует → ПРОПУСКАЕМ (уже писали)
        # Диалога нет → можно писать
        return False
    except Exception:
        # При ошибке — считаем что диалога нет, пробуем написать
        return False


async def dm_send_to_user(client, user_id: int, text: str) -> tuple[bool, str]:
    """
    Отправляет личное сообщение пользователю.
    Возвращает (success, error_reason).
    """
    try:
        await client.send_message(user_id, text)
        return True, ""
    except UserPrivacyRestrictedError:
        return False, "приватность"
    except UserIsBlockedError:
        return False, "заблокирован"
    except FloodWaitError as e:
        log(f"DM FloodWait {e.seconds}s")
        await asyncio.sleep(min(e.seconds, 120))
        try:
            await client.send_message(user_id, text)
            return True, ""
        except Exception as ex2:
            return False, str(ex2)[:60]
    except Exception as e:
        err = str(e)
        # Если ошибка связана с приватностью/доступом
        if any(x in err.lower() for x in ("privacy", "forbidden", "not mutual", "user_privacy")):
            return False, "приватность"
        return False, err[:60]


async def dm_task_run(notify_uid: int):
    """
    Фоновая задача:
    1. Сканирует указанные чаты → собирает активных участников
    2. Фильтрует уже получивших сообщение (по dm_sent)
    3. Проверяет открытость ЛС
    4. Пишет ТОЛЬКО новым — каждый человек получает сообщение максимум 1 раз
    5. Сохраняет всех обработанных в dm_sent (даже если ЛС закрыт — чтобы не пробовать снова)
    """
    dm_cfg["running"] = True
    sv(F["dm"], dm_cfg)

    text = dm_cfg.get("text", "").strip()
    chat_ids = dm_cfg.get("chats", [])
    top_n = int(dm_cfg.get("top_n", 10))
    limit = int(dm_cfg.get("scan_limit", 500))
    delay = float(dm_cfg.get("delay", 2.0))

    if not text:
        await bot.send_message(notify_uid, "❌ Текст для ЛС не задан!")
        dm_cfg["running"] = False
        sv(F["dm"], dm_cfg)
        return

    if not chat_ids:
        await bot.send_message(notify_uid, "❌ Чаты для анализа не указаны!")
        dm_cfg["running"] = False
        sv(F["dm"], dm_cfg)
        return

    aa = active()
    if not aa:
        await bot.send_message(notify_uid, "❌ Нет активных аккаунтов!")
        dm_cfg["running"] = False
        sv(F["dm"], dm_cfg)
        return

    client = clients[aa[0]["phone"]]

    await bot.send_message(
        notify_uid,
        f"🔍 Анализирую {len(chat_ids)} чат(ов), последние {limit} сообщений...\n"
        f"📂 В базе уже отправленных: <b>{len(dm_sent)}</b> чел.",
        parse_mode="HTML",
    )

    # ── Сбор активных участников со всех чатов ──
    all_users: dict = {}
    for cid in chat_ids:
        try:
            chat_id_int = int(cid)
        except (ValueError, TypeError):
            continue
        users = await get_active_users(client, chat_id_int, limit)
        for u in users:
            uid2 = u["user_id"]
            if uid2 in all_users:
                all_users[uid2]["count"] += u["count"]
            else:
                all_users[uid2] = dict(u)

    if not all_users:
        await bot.send_message(notify_uid, "😔 Не удалось найти активных участников.")
        dm_cfg["running"] = False
        sv(F["dm"], dm_cfg)
        return

    # ── Сортировка по активности ──
    sorted_users = sorted(all_users.values(), key=lambda x: x["count"], reverse=True)

    # ── Фильтр: убираем тех кому уже писали ──
    already_sent_ids = set(dm_sent.keys())  # строки str(user_id)
    new_users = [u for u in sorted_users if str(u["user_id"]) not in already_sent_ids]
    skipped_count = len(sorted_users) - len(new_users)

    if not new_users:
        await bot.send_message(
            notify_uid,
            f"ℹ️ Все {len(sorted_users)} найденных участников уже получили сообщение ранее.\n"
            f"Нажми <b>🗑 Сбросить базу</b> чтобы начать заново.",
            parse_mode="HTML",
        )
        dm_cfg["running"] = False
        sv(F["dm"], dm_cfg)
        return

    # Берём топ N из новых (не из всех!)
    top_users = new_users[:top_n]

    preview = "\n".join(
        f"  #{i+1} {u['name']} (@{u['username'] or '—'}) — {u['count']} сообщ."
        for i, u in enumerate(top_users)
    )
    await bot.send_message(
        notify_uid,
        f"📊 <b>Найдено новых: {len(new_users)}</b> (пропущено уже обработанных: {skipped_count})\n"
        f"✉️ Будет обработано топ <b>{len(top_users)}</b>:\n\n{preview}\n\n"
        f"🚀 Начинаю рассылку (пропускаю тех у кого уже открыт ЛС)...",
        parse_mode="HTML",
    )

    sent = 0
    failed = 0
    skipped_open = 0    # уже открытый ЛС → пропущен
    fail_reasons: list = []
    now_str = datetime.now().strftime("%d.%m %H:%M")

    for i, u in enumerate(top_users):
        if not dm_cfg.get("running"):
            await bot.send_message(notify_uid, f"🛑 Остановлено вручную. Отправлено: {sent}")
            break

        uid_str = str(u["user_id"])
        tag = f"@{u['username']}" if u["username"] else u["name"]

        # Проверяем: если ЛС с этим человеком уже открыт на аккаунте — ПРОПУСКАЕМ
        # (значит раньше уже писали или он писал нам)
        dm_already_open = await check_dm_open(client, u["user_id"])
        if dm_already_open:
            log(f"DM ⛔ {tag} — ЛС уже открыт, пропускаем")
            # Записываем в базу чтобы не трогать в следующий раз
            dm_sent[uid_str] = {
                "name": u["name"],
                "username": u["username"],
                "date": now_str,
                "ok": False,
                "reason": "ЛС уже открыт (пропущен)",
            }
            sv(F["sent_dm"], dm_sent)
            skipped_open += 1
            continue

        # Отправляем
        ok, reason = await dm_send_to_user(client, u["user_id"], text)
        tag_display = f"@{u['username']}" if u["username"] else u["name"]

        # В любом случае записываем в базу — чтобы не трогать повторно
        dm_sent[uid_str] = {
            "name": u["name"],
            "username": u["username"],
            "date": now_str,
            "ok": ok,
            "reason": reason if not ok else "",
        }
        sv(F["sent_dm"], dm_sent)   # сохраняем после каждой записи (защита от крашей)

        if ok:
            sent += 1
            log(f"DM ✅ {tag_display}")
        else:
            failed += 1
            fail_reasons.append(f"{tag_display}: {reason}")
            log(f"DM ❌ {tag_display} — {reason}")

        if delay > 0:
            await asyncio.sleep(delay)

    # ── Итоговая статистика ──
    run_time = datetime.now().strftime("%d.%m %H:%M")
    dm_stat["sent"] += sent
    dm_stat["fail"] += failed
    dm_stat["last_run"] = run_time
    dm_stat["log"].append({
        "d": run_time,
        "sent": sent,
        "fail": failed,
        "skipped": skipped_open,
        "chats": len(chat_ids),
        "top_n": len(top_users),
        "total_in_base": len(dm_sent),
    })
    if len(dm_stat["log"]) > 50:
        dm_stat["log"] = dm_stat["log"][-50:]
    sv(F["dmst"], dm_stat)

    result_msg = (
        f"✅ <b>Рассылка в ЛС завершена</b>\n\n"
        f"📤 Отправлено: <b>{sent}</b>\n"
        f"⛔ Пропущено (ЛС уже открыт): {skipped_open}\n"
        f"❌ Другие ошибки: {max(0, failed - skipped_open)}\n"
        f"📂 Всего в базе обработанных: <b>{len(dm_sent)}</b>\n\n"
        f"ℹ️ При следующем запуске все обработанные будут пропущены автоматически."
    )
    if fail_reasons:
        result_msg += "\n\n<b>Не доставлено:</b>\n" + "\n".join(fail_reasons[:8])

    await bot.send_message(notify_uid, result_msg, parse_mode="HTML")
    dm_cfg["running"] = False
    sv(F["dm"], dm_cfg)


# ══════════════════════════════════════════════════════════════
# ███  МОДУЛЬ: МАССОВАЯ РАССЫЛКА ПО БАЗЕ  ███
# ══════════════════════════════════════════════════════════════

def parse_users_from_text(text: str) -> list:
    """
    Парсит username или ID из текста.
    Форматы:
    - @username
    - username
    - 123456789 (числовой ID)
    - https://t.me/username
    
    Возвращает список строк (username без @ или ID как строка).
    """
    lines = text.strip().split("\n")
    result = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Убираем https://t.me/
        if "t.me/" in line:
            line = line.split("t.me/")[-1].split("?")[0].split("/")[0]
        # Убираем @
        if line.startswith("@"):
            line = line[1:]
        # Проверяем что не пустое
        if line:
            result.append(line)
    return result


async def mass_send_task(notify_uid: int):
    """
    Рассылает ЛС всем пользователям из загруженной базы.
    """
    mass_cfg["running"] = True
    sv(F["mass"], mass_cfg)
    
    text = mass_cfg.get("text", "").strip()
    users_raw = mass_cfg.get("users", [])
    delay = float(mass_cfg.get("delay", 2.0))
    use_base = mass_cfg.get("use_dm_sent", True)
    
    if not text:
        await bot.send_message(notify_uid, "❌ Текст не задан!")
        mass_cfg["running"] = False
        sv(F["mass"], mass_cfg)
        return
    
    if not users_raw:
        await bot.send_message(notify_uid, "❌ База пуста! Загрузи файл с пользователями.")
        mass_cfg["running"] = False
        sv(F["mass"], mass_cfg)
        return
    
    aa = active()
    if not aa:
        await bot.send_message(notify_uid, "❌ Нет активных аккаунтов!")
        mass_cfg["running"] = False
        sv(F["mass"], mass_cfg)
        return
    
    client = clients[aa[0]["phone"]]
    
    await bot.send_message(
        notify_uid,
        f"🚀 Начинаю массовую рассылку...\n"
        f"📋 Пользователей в базе: {len(users_raw)}\n"
        f"⏳ Задержка: {delay}с"
    )
    
    sent = 0
    failed = 0
    skipped = 0
    now_str = datetime.now().strftime("%d.%m %H:%M")
    
    for i, user_str in enumerate(users_raw):
        if not mass_cfg.get("running"):
            await bot.send_message(notify_uid, f"🛑 Остановлено. Отправлено: {sent}")
            break
        
        # Резолвим пользователя
        try:
            if user_str.isdigit():
                entity = await client.get_entity(int(user_str))
            else:
                entity = await client.get_entity(user_str)
        except Exception as e:
            log(f"MASS ❌ {user_str} — не найден: {e}")
            failed += 1
            continue
        
        if not isinstance(entity, User):
            failed += 1
            continue
        if getattr(entity, "bot", False):
            skipped += 1
            continue
        if getattr(entity, "deleted", False):
            failed += 1
            continue
        
        uid_str = str(entity.id)
        name_parts = [entity.first_name or "", entity.last_name or ""]
        name = " ".join(p for p in name_parts if p).strip() or f"id{entity.id}"
        tag = f"@{entity.username}" if entity.username else name
        
        # Проверяем базу
        if use_base and uid_str in dm_sent:
            log(f"MASS ⏭ {tag} — уже писали")
            skipped += 1
            continue
        
        # Отправляем
        ok, reason = await dm_send_to_user(client, entity.id, text)
        
        if use_base:
            dm_sent[uid_str] = {
                "name": name, "username": entity.username or "",
                "date": now_str, "ok": ok,
                "reason": reason if not ok else "",
                "src": "mass",
            }
            sv(F["sent_dm"], dm_sent)
        
        if ok:
            sent += 1
            log(f"MASS ✅ {tag}")
        else:
            failed += 1
            log(f"MASS ❌ {tag} — {reason}")
        
        if delay > 0:
            await asyncio.sleep(delay)
        
        # Прогресс каждые 10
        if (i + 1) % 10 == 0:
            try:
                await bot.send_message(
                    notify_uid,
                    f"📊 Прогресс: {i+1}/{len(users_raw)}\n✅{sent} ❌{failed} ⏭{skipped}"
                )
            except:
                pass
    
    # Финальный отчёт
    mass_stat["sent"] += sent
    mass_stat["fail"] += failed
    mass_stat["skip"] += skipped
    mass_stat["last_run"] = now_str
    
    await bot.send_message(
        notify_uid,
        f"✅ <b>Массовая рассылка завершена</b>\n\n"
        f"📤 Отправлено: <b>{sent}</b>\n"
        f"⏭ Пропущено (уже писали): {skipped}\n"
        f"❌ Ошибок: {failed}\n"
        f"📂 Всего в базе: {len(dm_sent)}",
        parse_mode="HTML"
    )
    
    mass_cfg["running"] = False
    sv(F["mass"], mass_cfg)


# ══════════════════════════════════════════════════════════════
# ███  МОДУЛЬ: ЖИВОЙ МОНИТОРИНГ (real-time → ЛС)  ███
# ══════════════════════════════════════════════════════════════

async def monitor_handle_new_message(event, client, notify_uids: list):
    """
    Вызывается при каждом новом сообщении в отслеживаемом чате.
    Если отправитель — живой человек которому ещё не писали — пишет ему в ЛС.
    """
    monitor_stat["events"] += 1

    # Получаем отправителя
    try:
        sender = await event.get_sender()
    except Exception:
        return

    if sender is None:
        return
    if not isinstance(sender, User):
        return
    if getattr(sender, "bot", False):
        return
    if getattr(sender, "deleted", False):
        return

    uid_str = str(sender.id)
    use_base = monitor_cfg.get("use_dm_sent", True)

    # Пропускаем если уже писали (и включена общая база)
    if use_base and uid_str in dm_sent:
        monitor_stat["skip"] += 1
        return

    # Получаем текст для ЛС
    text = monitor_cfg.get("text", "").strip()
    if not text:
        text = dm_cfg.get("text", "").strip()
    if not text:
        return  # текст не задан — ничего не делаем

    name_parts = [sender.first_name or "", sender.last_name or ""]
    name = " ".join(p for p in name_parts if p).strip() or f"id{sender.id}"
    tag  = f"@{sender.username}" if sender.username else name
    now_str = datetime.now().strftime("%d.%m %H:%M")

    # Небольшая задержка перед отправкой (антиспам ощущение)
    delay = float(monitor_cfg.get("delay", 1.5))
    if delay > 0:
        await asyncio.sleep(delay)

    # Проверяем доступность ЛС
    dm_open = await check_dm_open(client, sender.id)
    if not dm_open:
        log(f"MON ⛔ {tag} — ЛС недоступен")
        if use_base:
            dm_sent[uid_str] = {
                "name": name, "username": sender.username or "",
                "date": now_str, "ok": False, "reason": "недоступен", "src": "monitor",
            }
            sv(F["sent_dm"], dm_sent)
        monitor_stat["fail"] += 1
        return

    # Отправляем
    ok, reason = await dm_send_to_user(client, sender.id, text)

    # Записываем в базу в любом случае
    if use_base:
        dm_sent[uid_str] = {
            "name": name, "username": sender.username or "",
            "date": now_str, "ok": ok,
            "reason": reason if not ok else "",
            "src": "monitor",   # помечаем что пришло из мониторинга
        }
        sv(F["sent_dm"], dm_sent)

    if ok:
        monitor_stat["sent"] += 1
        log(f"MON ✅ {tag}")
        # Уведомление операторам
        for uid_b in notify_uids:
            try:
                await bot.send_message(
                    int(uid_b),
                    f"👁 <b>Мониторинг</b> | Новое сообщение → ЛС отправлен\n"
                    f"👤 {tag}",
                    parse_mode="HTML",
                )
            except Exception:
                pass
    else:
        monitor_stat["fail"] += 1
        log(f"MON ❌ {tag} — {reason}")


async def monitor_start(notify_uids: list) -> bool:
    """
    Регистрирует обработчики новых сообщений на всех активных аккаунтах
    для всех чатов из monitor_cfg["chats"].
    Возвращает True если хотя бы один обработчик установлен.
    """
    chat_ids = monitor_cfg.get("chats", [])
    if not chat_ids:
        return False

    aa = active()
    if not aa:
        return False

    # Конвертируем chat_ids в int, пропускаем невалидные
    parsed_ids = []
    for cid in chat_ids:
        try:
            parsed_ids.append(int(cid))
        except (ValueError, TypeError):
            pass

    if not parsed_ids:
        return False

    registered = 0
    for a in aa:
        c = clients[a["phone"]]

        # Снимаем старые обработчики этого клиента если были
        try:
            c.remove_event_handler(None)
        except Exception:
            pass

        # Регистрируем новый обработчик
        @c.on(events.NewMessage(chats=parsed_ids))
        async def _handler(event, _client=c, _uids=notify_uids):
            if not monitor_cfg.get("active"):
                return
            try:
                await monitor_handle_new_message(event, _client, _uids)
            except Exception as ex:
                log(f"MON handler err: {ex}")

        registered += 1
        log(f"MON 👁 {a.get('name', a['phone'])} слушает {len(parsed_ids)} чатов")

    return registered > 0


def monitor_stop():
    """Снимает все обработчики мониторинга со всех клиентов."""
    for ph, c in clients.items():
        try:
            c.remove_event_handler(None)
        except Exception:
            pass
    log("MON 🛑 Мониторинг остановлен")


# ══════════════════════════════════════════════════════════════

# ══════════════ КЛАВИАТУРЫ ══════════════
B = {
    "acc":     "👤 Аккаунты",
    "msg":     "💬 Сообщения",
    "chats":   "📋 Мои чаты",
    "set":     "⚙️ Настройки",
    "go":      "🚀 Запустить",
    "stop":    "🛑 Остановить",
    "pause":   "⏸ Пауза",
    "resume":  "▶️ Продолжить",
    "now":     "⚡ Сейчас",
    "stat":    "📊 Статистика",
    "log":     "📜 Лог",
    "back":    "◀️ Назад",
    "add":     "➕ Добавить",
    "reconn":  "🔄 Переподключить",
    "newmsg":  "✏️ Новое сообщение",
    "listmsg": "📋 Список",
    "rotate":  "🔄 Ротация",
    "attach":  "🖼 Медиа",
    "detach":  "📎 Убрать медиа",
    "delmsg":  "🗑 Удалить",
    "scan":    "🔍 Сканировать чаты",
    "scanpin": "📌 Сканировать закрепы",
    "allon":   "✅ Вкл все",
    "alloff":  "❌ Выкл все",
    "f_all":   "📂 Все",
    "f_gr":    "👥 Группы",
    "f_ch":    "📢 Каналы",
    "f_dm":    "👤 Лички",
    "f_bot":   "🤖 Боты",
    "f_pin":   "📌 С закрепом",
    "int":     "⏱ Интервал",
    "pin":     "📌 Режим",
    "delay":   "⏳ Задержка",
    "shuf":    "🔀 Порядок",
    "emoji":   "😎 Эмодзи",
    "adel":    "🗑 Автоудал",
    "sched":   "🕐 Расписание",
    "tpl":     "📝 Шаблоны",
    # Новый модуль — рассылка по топу
    "dm":        "💌 Активные → ЛС",
    "dm_text":   "✏️ Текст для ЛС",
    "dm_chat":   "📌 Добавить чат",
    "dm_clr":    "🗑 Очистить чаты",
    "dm_top":    "🔢 Кол-во топ",
    "dm_lim":    "📈 Лимит сканирования",
    "dm_go":     "🚀 Запустить ЛС-рассылку",
    "dm_stop":   "🛑 Стоп ЛС-рассылку",
    "dm_stat":   "📊 Статистика ЛС",
    "dm_prev":   "👁 Предпросмотр топ",
    "dm_base":   "📂 База отправленных",
    "dm_reset":  "♻️ Сбросить базу",
    # Мониторинг в реальном времени
    "mon":       "👁 Мониторинг",
    "mon_on":    "▶️ Включить мониторинг",
    "mon_off":   "⏹ Остановить мониторинг",
    "mon_chat":  "📌 Добавить чат (мон.)",
    "mon_clr":   "🗑 Очистить чаты (мон.)",
    "mon_text":  "✏️ Текст (мон.)",
    "mon_stat":  "📊 Статистика мониторинга",
    "mon_base":  "🔗 Использовать общую базу",
    # Массовая рассылка по базе
    "mass":      "📤 Массовая рассылка",
    "mass_load": "📁 Загрузить базу",
    "mass_text": "✏️ Текст (массов.)",
    "mass_go":   "🚀 Запустить рассылку",
    "mass_stop": "🛑 Остановить рассылку",
    "mass_stat": "📊 Стат. рассылки",
    "mass_base": "🔗 Общая база",
    "mass_view": "👁 Просмотр базы",
    "mass_clr":  "🗑 Очистить базу",
}

def rkm(b: list) -> RKM:
    return RKM(keyboard=[[KB(text=x)] for x in b], resize_keyboard=True)

def kb_main() -> RKM:
    b = [B["acc"], B["msg"], B["chats"], B["set"]]
    if S["run"] and not S["pause"]:
        b += [B["pause"], B["stop"]]
    elif S["pause"]:
        b += [B["resume"], B["stop"]]
    else:
        b += [B["go"]]
    b += [B["now"], B["stat"], B["log"], B["dm"], B["mon"], B["mass"]]
    return rkm(b)


def kb_mass() -> RKM:
    """Клавиатура модуля массовой рассылки."""
    running = mass_cfg.get("running", False)
    b = [B["mass_load"], B["mass_text"], B["mass_view"], B["mass_clr"], B["mass_base"]]
    if running:
        b.append(B["mass_stop"])
    else:
        b.append(B["mass_go"])
    b += [B["mass_stat"], B["back"]]
    return rkm(b)


def kb_monitor() -> RKM:
    """Клавиатура модуля живого мониторинга."""
    active_mon = monitor_cfg.get("active", False)
    b = [B["mon_text"], B["mon_chat"], B["mon_clr"], B["mon_base"]]
    if active_mon:
        b.append(B["mon_off"])
    else:
        b.append(B["mon_on"])
    b += [B["mon_stat"], B["back"]]
    return rkm(b)

def kb_acc() -> RKM:
    b = [B["add"], B["reconn"]]
    for i, a in enumerate(accs):
        cn = "🟢" if a["phone"] in clients else "🔴"
        on = "✅" if a.get("on", True) else "❌"
        b.append(f"{cn}{on} #{i+1} {a.get('name', '?')}")
    b.append(B["back"])
    return rkm(b)

def kb_msg() -> RKM:
    return rkm([
        B["newmsg"], B["listmsg"], B["rotate"],
        B["attach"], B["detach"], B["delmsg"], B["back"],
    ])

def kb_chats() -> RKM:
    b = [
        B["scan"], B["scanpin"], B["allon"], B["alloff"],
        B["f_all"], B["f_gr"], B["f_ch"], B["f_dm"], B["f_bot"], B["f_pin"],
    ]
    for i, a in enumerate(accs):
        b.append(f"👤#{i+1} {a.get('name', '?')[:12]}")
    b.append(B["back"])
    return rkm(b)

def kb_set() -> RKM:
    return rkm([
        B["int"], B["delay"], B["shuf"],
        B["emoji"], B["adel"], B["sched"], B["tpl"], B["back"],
    ])

def kb_dm() -> RKM:
    """Клавиатура модуля «Активные → ЛС»."""
    running = dm_cfg.get("running", False)
    b = [
        B["dm_text"], B["dm_chat"], B["dm_clr"],
        B["dm_top"], B["dm_lim"], B["dm_prev"],
        B["dm_base"], B["dm_reset"],
    ]
    if running:
        b.append(B["dm_stop"])
    else:
        b.append(B["dm_go"])
    b += [B["dm_stat"], B["back"]]
    return rkm(b)

# ══════════════ ПАГИНАЦИЯ ══════════════
def filtered_indices() -> list:
    ft = S.get("ft", "all")
    ft_acc = S.get("ft_acc", "all")
    out = []
    for i, sid in enumerate(tgt_list):
        t = targets.get(sid)
        if not t:
            continue
        tt = t.get("t", "")
        if ft == "gr"  and "гр" not in tt and "суп" not in tt: continue
        if ft == "ch"  and "кан" not in tt: continue
        if ft == "dm"  and "лс" not in tt:  continue
        if ft == "bot" and "бот" not in tt:  continue
        if ft == "pin" and not t.get("pin"): continue
        if ft_acc != "all" and t.get("ph", "") != ft_acc: continue
        out.append(i)
    return out

def chat_kb(page: int) -> IKM:
    idxs = filtered_indices()
    total = len(idxs)
    pages = max(1, math.ceil(total / PER_PAGE))
    page = max(0, min(page, pages - 1))
    start = page * PER_PAGE
    btns = []
    for pos in range(start, min(start + PER_PAGE, total)):
        i = idxs[pos]
        sid = tgt_list[i]
        t = targets[sid]
        on = "✅" if t.get("on") else "⬜"
        mi = t.get("mi", -1)
        mtxt = f"📝#{mi+1}" if 0 <= mi < len(msgs) else "📝общ"
        pin = "📌" if t.get("pin") else ""
        btns.append([
            IKB(text=f"{on}{pin} {t['t']} {t['n'][:14]}", callback_data=f"T{i}"),
            IKB(text=mtxt, callback_data=f"M{i}"),
        ])
    nav = []
    if page > 0:
        nav.append(IKB(text="⬅️", callback_data=f"P{page-1}"))
    nav.append(IKB(text=f"{page+1}/{pages}", callback_data="_"))
    if page < pages - 1:
        nav.append(IKB(text="➡️", callback_data=f"P{page+1}"))
    btns.append(nav)
    return IKM(inline_keyboard=btns)

async def show_chats(m, page: int = 0):
    S["page"] = page
    idxs = filtered_indices()
    on_cnt = sum(1 for i in idxs if targets.get(tgt_list[i], {}).get("on"))
    ft_name = {
        "all": "все", "gr": "группы", "ch": "каналы",
        "dm": "лички", "bot": "боты", "pin": "📌закреп",
    }.get(S.get("ft"), "все")
    ai = ""
    if S.get("ft_acc", "all") != "all":
        ai = f" | 👤{acc_short(S['ft_acc'])}"
    await m.answer(
        f"<b>📋 Чаты</b> | {len(idxs)} ({ft_name}{ai}) | ✅{on_cnt}",
        parse_mode="HTML",
        reply_markup=chat_kb(page),
    )

async def show_main(m):
    aa = active()
    on_cnt = sum(1 for t in targets.values() if t.get("on"))
    ci = sett.get("msg_idx", -1)
    cur = "—"
    if 0 <= ci < len(msgs):
        cur = msgs[ci]["text"][:40]
    elif msgs:
        cur = msgs[0]["text"][:40]
    st = "⏸" if S["run"] and S["pause"] else "🟢" if S["run"] else "🔴"
    media_st = f" | 🖼{S['mtype']}" if S["mtype"] else ""
    isec = sett.get("interval_sec", 300)
    await m.answer(
        f"<b>📨 PRO v14</b>\n\n"
        f"{st} 👤{len(aa)}/{len(accs)} 📋{on_cnt}/{len(targets)}\n"
        f"💬 <i>{cur}</i>{media_st}\n"
        f"⏱ {fmt_iv(isec)} | 📤{S['cnt']}",
        parse_mode="HTML",
        reply_markup=kb_main(),
    )

# ══════════════ HANDLERS ══════════════
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    uid = get_uid(m)
    if uid not in notify_users:
        notify_users.append(uid)
        sv(F["notify"], notify_users)
    set_wait(m, "")
    await show_main(m)

# ── Пустые callbacks ──
@dp.callback_query(lambda c: c.data == "_")
async def cb_noop(cb):
    try:
        await cb.answer()
    except Exception:
        pass

# ── Переключение чата on/off ──
@dp.callback_query(lambda c: c.data.startswith("T") and c.data[1:].isdigit())
async def cb_toggle(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    i = int(cb.data[1:])
    if i < len(tgt_list):
        sid = tgt_list[i]
        if sid in targets:
            targets[sid]["on"] = not targets[sid].get("on", False)
            sv(F["tgt"], targets)
        try:
            await cb.message.edit_reply_markup(reply_markup=chat_kb(S["page"]))
        except Exception:
            pass

# ── Выбор сообщения для чата ──
@dp.callback_query(lambda c: c.data.startswith("M") and c.data[1:].isdigit() and "D" not in c.data)
async def cb_pick(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    i = int(cb.data[1:])
    if i >= len(tgt_list):
        return
    sid = tgt_list[i]
    tgt = targets.get(sid)
    if not tgt or not msgs:
        await cb.message.answer("❌ Нет сообщений!")
        return
    btns = []
    for j, mg in enumerate(msgs):
        mark = "✅ " if tgt.get("mi") == j else ""
        btns.append([IKB(text=f"{mark}#{j+1}: {mg['text'][:30]}", callback_data=f"S{i}.{j}")])
    mark_def = "✅ " if tgt.get("mi", -1) < 0 else ""
    btns.append([IKB(text=f"{mark_def}📝 Общее", callback_data=f"S{i}.-1")])
    await cb.message.answer(
        f"💬 <b>{tgt['n']}</b> | 👤{acc_short(tgt.get('ph', ''))}",
        parse_mode="HTML",
        reply_markup=IKM(inline_keyboard=btns),
    )

@dp.callback_query(lambda c: c.data.startswith("S") and "." in c.data)
async def cb_setmsg(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    try:
        raw = cb.data[1:]
        dot = raw.index(".")
        i = int(raw[:dot])
        mi = int(raw[dot + 1:])
    except Exception:
        return
    if i >= len(tgt_list):
        return
    sid = tgt_list[i]
    tgt = targets.get(sid)
    if not tgt:
        return
    tgt["mi"] = mi
    sv(F["tgt"], targets)
    if 0 <= mi < len(msgs):
        await cb.message.answer(f"✅ <b>{tgt['n']}</b> → #{mi+1}", parse_mode="HTML")
    else:
        await cb.message.answer(f"✅ <b>{tgt['n']}</b> → общее", parse_mode="HTML")

@dp.callback_query(lambda c: c.data.startswith("P") and c.data[1:].isdigit())
async def cb_page(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    S["page"] = int(cb.data[1:])
    try:
        await cb.message.edit_reply_markup(reply_markup=chat_kb(S["page"]))
    except Exception:
        pass

@dp.callback_query(lambda c: c.data.startswith("IV"))
async def cb_IV(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    v = cb.data[2:]
    if v == "C":
        set_wait(cb, "interval")
        await cb.message.answer("Секунды (5–6000):")
    else:
        sett["interval_sec"] = int(v)
        sv(F["set"], sett)
        await cb.message.answer(f"✅ {fmt_iv(int(v))}")

@dp.callback_query(lambda c: c.data.startswith("DL"))
async def cb_DL(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    v = int(cb.data[2:])
    sett["auto_del"] = v
    sv(F["set"], sett)
    await cb.message.answer(f"🗑 {'выкл' if v == 0 else f'{v}с'}")

@dp.callback_query(lambda c: c.data.startswith("GM"))
async def cb_GM(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    i = int(cb.data[2:])
    if 0 <= i < len(msgs):
        sett["msg_idx"] = i
        sv(F["set"], sett)
        await cb.message.answer(f"✅ Общее = #{i+1}: {msgs[i]['text'][:50]}")

@dp.callback_query(lambda c: c.data.startswith("MD"))
async def cb_MD(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    v = cb.data[2:]
    if v == "A":
        msgs.clear()
        sett["msg_idx"] = -1
        for t in targets.values():
            t["mi"] = -1
        sv(F["tgt"], targets)
    else:
        j = int(v)
        if 0 <= j < len(msgs):
            msgs.pop(j)
            sett["msg_idx"] = -1 if not msgs else min(sett.get("msg_idx", 0), len(msgs) - 1)
            for t in targets.values():
                if t.get("mi", -1) == j:
                    t["mi"] = -1
                elif t.get("mi", -1) > j:
                    t["mi"] -= 1
            sv(F["tgt"], targets)
    sv(F["msg"], msgs)
    sv(F["set"], sett)
    await cb.message.answer(f"✅ Удалено! Осталось: {len(msgs)}")

@dp.callback_query(lambda c: c.data == "SR")
async def cb_SR(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    stat.update({"sent": 0, "err": 0, "skip": 0, "runs": 0, "hist": []})
    sv(F["stat"], stat)
    await cb.message.answer("✅ Статистика сброшена")

@dp.callback_query(lambda c: c.data.startswith("AT"))
async def cb_AT(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    i = int(cb.data[2:])
    if 0 <= i < len(accs):
        accs[i]["on"] = not accs[i].get("on", True)
        sv(F["acc"], accs)
        st = "✅ ВКЛ" if accs[i]["on"] else "❌ ВЫКЛ"
        await cb.message.answer(f"{st} {accs[i].get('name', '')}")

@dp.callback_query(lambda c: c.data.startswith("AX"))
async def cb_AX(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    i = int(cb.data[2:])
    if 0 <= i < len(accs):
        a = accs.pop(i)
        ph = a["phone"]
        if ph in clients:
            try:
                await clients[ph].disconnect()
            except Exception:
                pass
            del clients[ph]
        pin_cache.pop(ph, None)
        sv(F["acc"], accs)
        await cb.message.answer(f"🗑 {a.get('name', ph)} удалён")

@dp.callback_query(lambda c: c.data == "TN")
async def cb_TN(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    set_wait(cb, "tpl_n")
    await cb.message.answer("Название шаблона:")

@dp.callback_query(lambda c: c.data.startswith("TU"))
async def cb_TU(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    n = cb.data[2:]
    if n in tpl:
        msgs.append({
            "text": tpl[n],
            "created": f"{datetime.now():%d.%m %H:%M}",
            "used": 0,
        })
        sett["msg_idx"] = len(msgs) - 1
        sv(F["msg"], msgs)
        sv(F["set"], sett)
        await cb.message.answer(f"✅ «{n}» → #{len(msgs)}")

@dp.callback_query(lambda c: c.data == "TD")
async def cb_TD(cb):
    try:
        await cb.answer()
    except Exception:
        pass
    set_wait(cb, "tpl_d")
    await cb.message.answer(f"Имя шаблона для удаления:\n{', '.join(tpl)}")

# ── Фото / документ ──
@dp.message(lambda m: m.photo is not None)
async def h_ph(m: types.Message):
    if get_wait(m) == "photo":
        f = await bot.get_file(m.photo[-1].file_id)
        p = DATA / "media.jpg"
        await bot.download_file(f.file_path, str(p))
        S["mupl"] = {}
        for a in active():
            try:
                S["mupl"][a["phone"]] = await clients[a["phone"]].upload_file(str(p))
            except Exception:
                pass
        S["mtype"] = "photo"
        set_wait(m, "")
        await m.answer(f"✅ Фото загружено → {len(S['mupl'])} аккаунт(ов)")

@dp.message(lambda m: m.document is not None)
async def h_dc(m: types.Message):
    w = get_wait(m)
    
    # Загрузка базы пользователей из .txt
    if w == "mass_load":
        f = await bot.get_file(m.document.file_id)
        fn = m.document.file_name or "file"
        
        if not fn.endswith(".txt"):
            await m.answer("❌ Нужен .txt файл (текстовый)")
            return
        
        p = DATA / "temp_users.txt"
        await bot.download_file(f.file_path, str(p))
        
        try:
            content = p.read_text("utf-8")
        except:
            await m.answer("❌ Ошибка чтения файла (проверь кодировку UTF-8)")
            p.unlink()
            return
        
        users = parse_users_from_text(content)
        p.unlink()
        
        if not users:
            await m.answer("❌ Не удалось распознать пользователей в файле")
            return
        
        mass_cfg["users"] = users
        sv(F["mass"], mass_cfg)
        F["mass_users"].write_text("\n".join(users), "utf-8")
        set_wait(m, "")
        
        preview = "\n".join(f"  {i+1}. {u}" for i, u in enumerate(users[:15]))
        if len(users) > 15:
            preview += f"\n  ... и ещё {len(users)-15}"
        
        await m.answer(
            f"✅ Загружено из файла: <b>{len(users)}</b> польз.\n\n{preview}",
            parse_mode="HTML"
        )
        return
    
    # Остальная логика медиа
    if get_wait(m) == "photo":
        f = await bot.get_file(m.document.file_id)
        fn = m.document.file_name or "file"
        p = DATA / fn
        await bot.download_file(f.file_path, str(p))
        S["mupl"] = {}
        for a in active():
            try:
                S["mupl"][a["phone"]] = await clients[a["phone"]].upload_file(str(p))
            except Exception:
                pass
        S["mtype"] = "doc"
        set_wait(m, "")
        await m.answer(f"✅ «{fn}» загружен → {len(S['mupl'])} аккаунт(ов)")

# ══════════════ ГЛАВНЫЙ ТЕКСТОВЫЙ HANDLER ══════════════
@dp.message()
async def handle(m: types.Message):
    t = (m.text or "").strip()
    uid = get_uid(m)

    # Регистрируем для уведомлений
    if uid not in notify_users:
        notify_users.append(uid)
        sv(F["notify"], notify_users)

    w = get_wait(m)
    skip = set(B.values())

    # ═══ ВВОД ДАННЫХ (ожидаемые ответы) ═══
    if w and t not in skip:

        # --- Добавление аккаунта ---
        if w == "acc_ph":
            parts = t.split()
            if len(parts) < 3:
                await m.answer("❌ Формат: <code>+7... api_id api_hash</code>", parse_mode="HTML")
                return
            ph, aid, ahash = parts[0], parts[1], parts[2]
            
            # Валидация формата номера
            if not ph.startswith("+"):
                await m.answer("❌ Номер должен начинаться с + (например: +79991234567)")
                return
            
            S["tmp"][uid] = {"ph": ph, "aid": aid, "ah": ahash}
            sess = f"s_{ph.replace('+', '')}"
            try:
                c = TelegramClient(str(DATA / sess), int(aid), ahash)
                await c.connect()
                if await c.is_user_authorized():
                    me = await c.get_me()
                    accs.append({
                        "phone": ph, "api_id": aid, "api_hash": ahash,
                        "session": sess,
                        "name": f"{me.first_name} @{me.username or '?'}",
                        "on": True,
                    })
                    sv(F["acc"], accs)
                    clients[ph] = c
                    pin_cache[ph] = {}
                    set_wait(m, "")
                    await m.answer(f"✅ {me.first_name}!")
                else:
                    # Отправляем код
                    await m.answer(
                        "📲 <b>Отправляю код авторизации...</b>\n\n"
                        "⏳ Подожди 10-30 секунд\n\n"
                        "Код придёт:\n"
                        "• В Telegram на этот номер\n"
                        "• ИЛИ по SMS\n\n"
                        "⚠️ <b>Важно:</b> код НЕ придёт сюда в бот!",
                        parse_mode="HTML"
                    )
                    r = await c.send_code_request(ph)
                    S["tmp"][uid].update({
                        "hash": r.phone_code_hash, "client": c, "sess": sess,
                    })
                    set_wait(m, "acc_code")
                    await m.answer(
                        "✅ Запрос отправлен!\n\n"
                        "📱 <b>Открой Telegram на своём телефоне</b>\n"
                        "Код должен прийти туда (не сюда в бот)\n\n"
                        "✍️ <b>Набери код ВРУЧНУЮ С ПРОБЕЛАМИ:</b>\n"
                        "<code>1 2 3 4 5</code>\n\n"
                        "❌ <b>НЕ копируй код</b> — только набирай руками!\n"
                        "❌ <b>НЕ пересылай</b> код сюда!\n\n"
                        "⏱ Если код не пришёл в Telegram — подожди SMS (может занять 1-2 минуты)",
                        parse_mode="HTML"
                    )
            except Exception as e:
                set_wait(m, "")
                await m.answer(f"❌ Ошибка подключения:\n<code>{str(e)[:300]}</code>", parse_mode="HTML")
            return

        if w == "acc_code":
            code = t.replace(" ", "").replace("-", "")
            d = S["tmp"].get(uid, {})
            c = d.get("client")
            if not c:
                set_wait(m, "")
                return
            try:
                await c.sign_in(d["ph"], code, phone_code_hash=d["hash"])
                me = await c.get_me()
                accs.append({
                    "phone": d["ph"], "api_id": d["aid"], "api_hash": d["ah"],
                    "session": d["sess"],
                    "name": f"{me.first_name} @{me.username or '?'}",
                    "on": True,
                })
                sv(F["acc"], accs)
                clients[d["ph"]] = c
                pin_cache[d["ph"]] = {}
                set_wait(m, "")
                await m.answer(f"✅ {me.first_name}!")
            except SessionPasswordNeededError:
                set_wait(m, "acc_2fa")
                await m.answer("🔐 Введи пароль 2FA:")
            except PhoneCodeInvalidError:
                await m.answer("❌ Неверный код! Попробуй снова с пробелами:")
            except Exception as e:
                set_wait(m, "")
                await m.answer(f"❌ {e}")
            return

        if w == "acc_2fa":
            d = S["tmp"].get(uid, {})
            c = d.get("client")
            if not c:
                set_wait(m, "")
                return
            try:
                await c.sign_in(password=t)
                me = await c.get_me()
                accs.append({
                    "phone": d["ph"], "api_id": d["aid"], "api_hash": d["ah"],
                    "session": d["sess"],
                    "name": f"{me.first_name} @{me.username or '?'}",
                    "on": True,
                })
                sv(F["acc"], accs)
                clients[d["ph"]] = c
                pin_cache[d["ph"]] = {}
                set_wait(m, "")
                await m.answer(f"✅ {me.first_name}!")
            except PasswordHashInvalidError:
                await m.answer("❌ Неверный пароль 2FA!")
            except Exception as e:
                set_wait(m, "")
                await m.answer(f"❌ {e}")
            return

        if w == "msg":
            msgs.append({"text": t, "created": f"{datetime.now():%d.%m %H:%M}", "used": 0})
            sett["msg_idx"] = len(msgs) - 1
            sv(F["msg"], msgs)
            sv(F["set"], sett)
            set_wait(m, "")
            await m.answer(f"✅ Сообщение #{len(msgs)} сохранено")
            return

        if w == "interval":
            try:
                v = int(t)
                if 5 <= v <= 6000:
                    sett["interval_sec"] = v
                    sv(F["set"], sett)
                    set_wait(m, "")
                    await m.answer(f"✅ Интервал: {fmt_iv(v)}")
                else:
                    await m.answer("Введи число от 5 до 6000:")
            except ValueError:
                await m.answer("❌ Только цифры!")
            return

        if w == "delay":
            try:
                v = float(t)
                if 0 <= v <= 10:
                    sett["delay"] = v
                    sv(F["set"], sett)
                    set_wait(m, "")
                    await m.answer(f"✅ Задержка: {v}с")
                else:
                    await m.answer("Введи от 0 до 10:")
            except ValueError:
                await m.answer("❌ Только число!")
            return

        if w == "schedule":
            if t == "0":
                sett["schedule"] = []
            else:
                try:
                    sett["schedule"] = [
                        h for h in (int(x.strip()) for x in t.split(","))
                        if 0 <= h <= 23
                    ]
                except ValueError:
                    await m.answer("❌ Пример: <code>9,12,18</code>", parse_mode="HTML")
                    return
            sv(F["set"], sett)
            set_wait(m, "")
            s = sett["schedule"]
            await m.answer(
                f"✅ {', '.join(f'{h}:00' for h in sorted(s)) if s else '24/7'}"
            )
            return

        if w == "tpl_n":
            S["tmp"].setdefault(uid, {})["tn"] = t
            set_wait(m, "tpl_t")
            await m.answer("Введи текст шаблона:")
            return

        if w == "tpl_t":
            n = S["tmp"].get(uid, {}).get("tn", "?")
            tpl[n] = t
            sv(F["tpl"], tpl)
            set_wait(m, "")
            await m.answer(f"✅ Шаблон «{n}» сохранён")
            return

        if w == "tpl_d":
            if t in tpl:
                del tpl[t]
                sv(F["tpl"], tpl)
                await m.answer(f"🗑 Шаблон «{t}» удалён")
            else:
                await m.answer("❌ Шаблон не найден")
            set_wait(m, "")
            return

        # ═══ Массовая рассылка: текст ═══
        if w == "mass_text":
            mass_cfg["text"] = t
            sv(F["mass"], mass_cfg)
            set_wait(m, "")
            await m.answer(f"✅ Текст сохранён:\n\n<i>{t[:200]}</i>", parse_mode="HTML")
            return

        # ═══ Массовая рассылка: загрузка базы текстом ═══
        if w == "mass_load":
            users = parse_users_from_text(t)
            if not users:
                await m.answer("❌ Не удалось распознать пользователей. Проверь формат.")
                return
            mass_cfg["users"] = users
            sv(F["mass"], mass_cfg)
            # Сохраняем в файл для постоянства
            F["mass_users"].write_text("\n".join(users), "utf-8")
            set_wait(m, "")
            preview = "\n".join(f"  {i+1}. {u}" for i, u in enumerate(users[:10]))
            if len(users) > 10:
                preview += f"\n  ... и ещё {len(users)-10}"
            await m.answer(
                f"✅ Загружено: <b>{len(users)}</b> польз.\n\n{preview}",
                parse_mode="HTML"
            )
            return

        # ═══ Мониторинг: ввод текста ═══
        if w == "mon_text":
            if t.strip() == "-":
                monitor_cfg["text"] = ""
                sv(F["monitor"], monitor_cfg)
                set_wait(m, "")
                await m.answer("✅ Текст мониторинга очищен — будет использоваться общий текст из «Активные → ЛС»")
            else:
                monitor_cfg["text"] = t
                sv(F["monitor"], monitor_cfg)
                set_wait(m, "")
                await m.answer(f"✅ Текст для мониторинга сохранён:\n\n<i>{t[:200]}</i>", parse_mode="HTML")
            # Если мониторинг активен — перезапускаем с новым текстом (текст читается динамически, перезапуск не нужен)
            return

        # ═══ Мониторинг: добавление чата ═══
        if w == "mon_chat":
            raw = t.strip()
            chat_id_str = None
            try:
                chat_id_str = str(int(raw))
            except ValueError:
                aa = active()
                if aa:
                    try:
                        entity = await clients[aa[0]["phone"]].get_entity(raw)
                        chat_id_str = str(entity.id)
                    except Exception as e:
                        await m.answer(f"❌ Не удалось найти чат: {e}")
                        set_wait(m, "")
                        return
                else:
                    await m.answer("❌ Нет активных аккаунтов для поиска чата")
                    set_wait(m, "")
                    return
            if chat_id_str:
                if chat_id_str not in monitor_cfg["chats"]:
                    monitor_cfg["chats"].append(chat_id_str)
                    sv(F["monitor"], monitor_cfg)
                    await m.answer(
                        f"✅ Чат {chat_id_str} добавлен в мониторинг.\n"
                        f"Всего: {len(monitor_cfg['chats'])}\n\n"
                        f"{'⚠️ Перезапусти мониторинг чтобы применить изменения.' if monitor_cfg.get('active') else ''}"
                    )
                else:
                    await m.answer("ℹ️ Этот чат уже добавлен в мониторинг")
            set_wait(m, "")
            return

        # ═══ DM-модуль: ввод данных ═══
        if w == "dm_text":
            dm_cfg["text"] = t
            sv(F["dm"], dm_cfg)
            set_wait(m, "")
            await m.answer(f"✅ Текст для ЛС сохранён:\n\n<i>{t[:200]}</i>", parse_mode="HTML")
            return

        if w == "dm_chat":
            # Принимаем chat_id (число) или @username или https://t.me/...
            raw = t.strip()
            chat_id_str = None
            try:
                chat_id_str = str(int(raw))  # уже число
            except ValueError:
                # Попробуем разрезолвить через Telethon
                aa = active()
                if aa:
                    try:
                        entity = await clients[aa[0]["phone"]].get_entity(raw)
                        chat_id_str = str(entity.id)
                    except Exception as e:
                        await m.answer(f"❌ Не удалось найти чат: {e}")
                        set_wait(m, "")
                        return
                else:
                    await m.answer("❌ Нет активных аккаунтов для поиска чата")
                    set_wait(m, "")
                    return
            if chat_id_str:
                if chat_id_str not in dm_cfg["chats"]:
                    dm_cfg["chats"].append(chat_id_str)
                    sv(F["dm"], dm_cfg)
                    await m.answer(f"✅ Чат {chat_id_str} добавлен. Всего: {len(dm_cfg['chats'])}")
                else:
                    await m.answer("ℹ️ Этот чат уже добавлен")
            set_wait(m, "")
            return

        if w == "dm_top":
            try:
                v = int(t)
                if 1 <= v <= 200:
                    dm_cfg["top_n"] = v
                    sv(F["dm"], dm_cfg)
                    set_wait(m, "")
                    await m.answer(f"✅ Топ: {v} участников")
                else:
                    await m.answer("Введи от 1 до 200:")
            except ValueError:
                await m.answer("❌ Только число!")
            return

        if w == "dm_lim":
            try:
                v = int(t)
                if 10 <= v <= 5000:
                    dm_cfg["scan_limit"] = v
                    sv(F["dm"], dm_cfg)
                    set_wait(m, "")
                    await m.answer(f"✅ Лимит сканирования: {v} сообщений")
                else:
                    await m.answer("Введи от 10 до 5000:")
            except ValueError:
                await m.answer("❌ Только число!")
            return

    # ═══ КНОПКИ ГЛАВНОГО МЕНЮ ═══

    if t == B["back"]:
        set_wait(m, "")
        await show_main(m)
        return

    if t == B["acc"]:
        set_wait(m, "")
        lines = ["<b>👤 Аккаунты</b>\n"]
        for i, a in enumerate(accs):
            cn = "🟢" if a["phone"] in clients else "🔴"
            on = "✅" if a.get("on", True) else "❌"
            cnt = sum(1 for tg in targets.values() if tg.get("ph") == a["phone"] and tg.get("on"))
            lines.append(f"{cn}{on} <b>#{i+1}</b> {a.get('name', '?')}\n   <code>{a['phone']}</code> | 📋{cnt} чатов")
        await m.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb_acc())
        return

    if t == B["msg"]:
        set_wait(m, "")
        ci = sett.get("msg_idx", -1)
        txt = f"<b>💬 Сообщения ({len(msgs)})</b>\n"
        if S["mtype"]:
            txt += f"🖼 Медиа: {S['mtype']}\n"
        txt += "\n"
        if msgs:
            for i, mg in enumerate(msgs):
                act = " 👈 ОБЩЕЕ" if i == ci else ""
                txt += f"<b>#{i+1}</b> {mg['text'][:40].replace('<', '&lt;')}{act}\n"
        else:
            txt += "Список пуст\n"
        await m.answer(txt, parse_mode="HTML", reply_markup=kb_msg())
        if msgs:
            btns = [
                [IKB(text=f"{'✅' if i == ci else ''}#{i+1}", callback_data=f"GM{i}")]
                for i in range(len(msgs))
            ]
            await m.answer("Выбери общее сообщение:", reply_markup=IKM(inline_keyboard=btns))
        return

    if t == B["chats"]:
        set_wait(m, "")
        S["ft"] = "all"
        S["ft_acc"] = "all"
        if not targets:
            await m.answer("Пусто! Нажми 🔍 Сканировать чаты", reply_markup=kb_chats())
        else:
            await m.answer("📋", reply_markup=kb_chats())
            await show_chats(m, 0)
        return

    if t == B["set"]:
        set_wait(m, "")
        isec = sett.get("interval_sec", 300)
        ad = sett.get("auto_del", 0)
        sc = sett.get("schedule", [])
        await m.answer(
            f"<b>⚙️ Настройки</b>\n"
            f"⏱ Интервал: {fmt_iv(isec)}\n"
            f"⏳ Задержка: {sett.get('delay', 0.3)}с\n"
            f"🔀 Перемешать: {'✅' if sett.get('shuffle') else '❌'}\n"
            f"😎 Эмодзи: {'✅' if sett.get('emoji') else '❌'}\n"
            f"🗑 Автоудаление: {'выкл' if ad == 0 else f'{ad}с'}\n"
            f"🕐 Расписание: {', '.join(f'{h}:00' for h in sorted(sc)) if sc else '24/7'}",
            parse_mode="HTML",
            reply_markup=kb_set(),
        )
        return

    if t == B["go"]:
        if S["run"]:
            await m.answer("Рассылка уже запущена!")
            return
        on = [s for s in targets if targets[s].get("on")]
        if not on:
            await m.answer("❌ Включи хотя бы один чат!", reply_markup=kb_main())
            return
        if not msgs:
            await m.answer("❌ Добавь сообщение!", reply_markup=kb_main())
            return
        if not active():
            await m.answer("❌ Подключи аккаунт!", reply_markup=kb_main())
            return
        S["run"] = True
        S["pause"] = False
        S["cnt"] = 0
        await m.answer(
            f"🚀 Запущено!\n👤 {len(active())} акк | 📋 {len(on)} чатов | ⏱ {fmt_iv(sett.get('interval_sec', 300))}",
            reply_markup=kb_main(),
        )
        S["task"] = asyncio.create_task(loop())
        return

    if t == B["stop"]:
        if not S["run"]:
            return
        S["run"] = False
        S["pause"] = False
        if S["task"]:
            S["task"].cancel()
            S["task"] = None
        await m.answer(f"🛑 Остановлено. Отправлено: {S['cnt']}", reply_markup=kb_main())
        return

    if t == B["pause"]:
        S["pause"] = True
        await m.answer("⏸ Пауза", reply_markup=kb_main())
        return

    if t == B["resume"]:
        S["pause"] = False
        await m.answer("▶️ Продолжено", reply_markup=kb_main())
        return

    if t == B["now"]:
        if not msgs or not any(targets[s].get("on") for s in targets):
            await m.answer("❌ Нечего отправлять")
            return
        if S["run"]:
            S["force"] = True
            await m.answer("⚡ Принудительная отправка...")
        else:
            await m.answer("⚡ Отправляю...")
            s, sk, e, errs = await broadcast()
            r = f"📤 ✅{s} ⏭{sk} ❌{e}"
            if errs:
                r += "\n" + "\n".join(errs[:3])
            await m.answer(r, parse_mode="HTML")
        return

    if t == B["stat"]:
        timer = "—"
        if S["run"] and S["next"]:
            s2 = max(0, int((S["next"] - datetime.now()).total_seconds()))
            mn, sc = divmod(s2, 60)
            timer = f"⏳{mn:02d}:{sc:02d}"
        h = stat.get("hist", [])[-5:]
        ht = "\n".join(
            f"  {x['d']} ✅{x['s']}⏭{x['sk']}❌{x['e']}" for x in reversed(h)
        ) or "  —"
        await m.answer(
            f"<b>📊 Статистика</b>\n"
            f"✅ Отправлено: {stat['sent']}\n"
            f"⏭ Пропущено: {stat['skip']}\n"
            f"❌ Ошибок: {stat['err']}\n"
            f"🔄 Запусков: {stat['runs']}\n"
            f"{timer}\n\n"
            f"<b>Последние 5:</b>\n{ht}",
            parse_mode="HTML",
            reply_markup=IKM(inline_keyboard=[[IKB(text="🗑 Сбросить", callback_data="SR")]]),
        )
        return

    if t == B["log"]:
        if F["log"].exists() and F["log"].stat().st_size > 0:
            await m.answer_document(FSInputFile(str(F["log"])))
        else:
            await m.answer("Лог пуст.")
        return

    if t == B["add"]:
        set_wait(m, "acc_ph")
        await m.answer(
            "📱 <b>Добавить аккаунт</b>\n\n"
            "Отправь в одну строку:\n"
            "<code>+7... api_id api_hash</code>\n\n"
            "⚠️ Код потом набирай <b>с пробелами:</b>\n"
            "<code>1 2 3 4 5</code>\n"
            "❌ НЕ копируй!",
            parse_mode="HTML",
        )
        return

    if t == B["reconn"]:
        await m.answer("🔄 Переподключаю...")
        ok = await conn_all()
        await m.answer(f"✅ Подключено: {ok}/{len(accs)}", reply_markup=kb_acc())
        return

    # Выбор аккаунта из списка
    if t.startswith(("🟢", "🔴")) and "#" in t:
        try:
            idx = int(t.split("#")[1].split()[0]) - 1
            if 0 <= idx < len(accs):
                a = accs[idx]
                on = a.get("on", True)
                cnt = sum(1 for tg in targets.values() if tg.get("ph") == a["phone"])
                await m.answer(
                    f"<b>#{idx+1} {a.get('name', '?')}</b>\n"
                    f"{'✅ ВКЛ' if on else '❌ ВЫКЛ'} | {cnt} чатов",
                    parse_mode="HTML",
                    reply_markup=IKM(inline_keyboard=[[
                        IKB(text="❌ Выкл" if on else "✅ Вкл", callback_data=f"AT{idx}"),
                        IKB(text="🗑 Удалить", callback_data=f"AX{idx}"),
                    ]]),
                )
        except (ValueError, IndexError):
            pass
        return

    if t == B["newmsg"]:
        set_wait(m, "msg")
        await m.answer("✏️ Введи текст сообщения:")
        return

    if t == B["listmsg"]:
        if not msgs:
            await m.answer("📭 Сообщений нет")
            return
        for i, mg in enumerate(msgs):
            await m.answer(f"<b>#{i+1}</b>\n{mg['text'][:500]}", parse_mode="HTML")
        return

    if t == B["rotate"]:
        sett["rotate_on"] = not sett.get("rotate_on", False)
        sv(F["set"], sett)
        if sett["rotate_on"]:
            if sett.get("msg_idx", -1) < 0 and msgs:
                sett["msg_idx"] = 0
                sv(F["set"], sett)
            await m.answer(
                f"🔄 Ротация ВКЛ — каждый раз следующее сообщение\n"
                f"Сейчас: #{sett.get('msg_idx', 0)+1}/{len(msgs)}"
            )
        else:
            await m.answer(f"🔄 Ротация ВЫКЛ — всегда #{sett.get('msg_idx', 0)+1}")
        return

    if t == B["attach"]:
        set_wait(m, "photo")
        await m.answer("🖼 Отправь фото или файл:")
        return

    if t == B["detach"]:
        S["mtype"] = None
        S["mupl"] = {}
        await m.answer("📎 Медиа убрано")
        return

    if t == B["delmsg"]:
        if not msgs:
            await m.answer("📭 Нет сообщений")
            return
        btns = [
            [IKB(text=f"🗑#{i+1} {mg['text'][:20]}", callback_data=f"MD{i}")]
            for i, mg in enumerate(msgs)
        ]
        btns.append([IKB(text="🗑 Удалить ВСЁ", callback_data="MDA")])
        await m.answer("Выбери что удалить:", reply_markup=IKM(inline_keyboard=btns))
        return

    if t == B["scan"]:
        await m.answer("🔍 Сканирую чаты (включая архив)...")
        nw, total, scanned = await scan_all()
        pin_cnt = sum(1 for tg in targets.values() if tg.get("pin"))
        await m.answer(
            f"✅ Новых: +{nw}\nВсего чатов: <b>{total}</b>\n📌 С закрепом: {pin_cnt}\n\n"
            f"<i>Для обновления закрепов нажми 📌 Сканировать закрепы</i>",
            parse_mode="HTML",
        )
        await show_chats(m, 0)
        return

    if t == B["scanpin"]:
        await m.answer("📌 Сканирую закрепы (может занять время)...")
        cnt = await scan_pins()
        await m.answer(f"📌 Найдено закрепов: <b>{cnt}</b>", parse_mode="HTML")
        await show_chats(m, S["page"])
        return

    if t == B["allon"]:
        idxs = filtered_indices()
        for i in idxs:
            targets[tgt_list[i]]["on"] = True
        sv(F["tgt"], targets)
        await m.answer(f"✅ Включено: {len(idxs)}")
        await show_chats(m, S["page"])
        return

    if t == B["alloff"]:
        idxs = filtered_indices()
        for i in idxs:
            targets[tgt_list[i]]["on"] = False
        sv(F["tgt"], targets)
        await m.answer(f"❌ Выключено: {len(idxs)}")
        await show_chats(m, S["page"])
        return

    for key, ft in [
        ("f_all", "all"), ("f_gr", "gr"), ("f_ch", "ch"),
        ("f_dm", "dm"), ("f_bot", "bot"), ("f_pin", "pin"),
    ]:
        if t == B[key]:
            S["ft"] = ft
            S["ft_acc"] = "all"
            S["page"] = 0
            await show_chats(m, 0)
            return

    for i, a in enumerate(accs):
        if t == f"👤#{i+1} {a.get('name', '?')[:12]}":
            S["ft_acc"] = a["phone"]
            S["ft"] = "all"
            S["page"] = 0
            await show_chats(m, 0)
            return

    if t == B["int"]:
        isec = sett.get("interval_sec", 300)
        btns = [
            [IKB(text=f"{v}с", callback_data=f"IV{v}") for v in [5, 10, 15, 30, 45, 60]],
            [IKB(text=f"{v}м", callback_data=f"IV{v*60}") for v in [1, 2, 3, 5, 10, 15]],
            [IKB(text=f"{v}м", callback_data=f"IV{v*60}") for v in [30, 45, 60, 90, 100]],
            [IKB(text="✏️ Вручную", callback_data="IVC")],
        ]
        await m.answer(f"⏱ Текущий: {fmt_iv(isec)}", reply_markup=IKM(inline_keyboard=btns))
        return

    if t == B["delay"]:
        set_wait(m, "delay")
        await m.answer(f"⏳ Текущая задержка: {sett.get('delay', 0.3)}с\nВведи новую (0–10):")
        return

    if t == B["shuf"]:
        sett["shuffle"] = not sett.get("shuffle", False)
        sv(F["set"], sett)
        await m.answer(f"🔀 {'✅ Перемешивать чаты' if sett['shuffle'] else '❌ Без перемешивания'}")
        return

    if t == B["emoji"]:
        sett["emoji"] = not sett.get("emoji", False)
        sv(F["set"], sett)
        await m.answer(f"😎 {'✅ Эмодзи добавляются' if sett['emoji'] else '❌ Без эмодзи'}")
        return

    if t == B["adel"]:
        btns = [
            [IKB(text=x, callback_data=f"DL{v}") for x, v in [("Выкл", "0"), ("30с", "30"), ("1м", "60")]],
            [IKB(text=x, callback_data=f"DL{v}") for x, v in [("5м", "300"), ("10м", "600"), ("30м", "1800")]],
        ]
        await m.answer("🗑 Автоудаление через:", reply_markup=IKM(inline_keyboard=btns))
        return

    if t == B["sched"]:
        set_wait(m, "schedule")
        sc = sett.get("schedule", [])
        await m.answer(
            f"🕐 Текущее: {', '.join(f'{h}:00' for h in sorted(sc)) if sc else '24/7'}\n\n"
            f"Введи часы через запятую:\n<code>9,12,18</code>\nИли <code>0</code> для 24/7",
            parse_mode="HTML",
        )
        return

    if t == B["tpl"]:
        txt = f"<b>📝 Шаблоны ({len(tpl)})</b>\n\n"
        btns = []
        for n, tv in tpl.items():
            txt += f"<b>{n}</b>: {tv[:40]}\n"
            btns.append([IKB(text=f"📝 {n}", callback_data=f"TU{n}")])
        btns.append([IKB(text="➕ Новый", callback_data="TN")])
        if tpl:
            btns.append([IKB(text="🗑 Удалить", callback_data="TD")])
        await m.answer(txt, parse_mode="HTML", reply_markup=IKM(inline_keyboard=btns))
        return

    # ══════════════════════════════════════════════════
    # ███  ОБРАБОТЧИКИ МОДУЛЯ «АКТИВНЫЕ → ЛС»  ███
    # ══════════════════════════════════════════════════

    if t == B["dm"]:
        set_wait(m, "")
        running = dm_cfg.get("running", False)
        chats_list = dm_cfg.get("chats", [])
        sent_total = len(dm_sent)
        sent_ok = sum(1 for v in dm_sent.values() if v.get("ok"))
        sent_fail = sent_total - sent_ok
        txt = (
            f"<b>💌 Рассылка активным участникам</b>\n\n"
            f"📌 Чатов для анализа: {len(chats_list)}\n"
            f"🔢 Топ участников: {dm_cfg.get('top_n', 10)}\n"
            f"📈 Анализировать сообщений: {dm_cfg.get('scan_limit', 500)}\n"
            f"⏳ Задержка между ЛС: {dm_cfg.get('delay', 2.0)}с\n\n"
            f"📂 <b>База обработанных: {sent_total} чел.</b> "
            f"(✅{sent_ok} отправлено, ❌{sent_fail} не дошло)\n"
            f"<i>Все они будут пропущены при следующем запуске</i>\n\n"
        )
        if chats_list:
            txt += "📋 <b>Чаты:</b>\n" + "\n".join(f"  • {cid}" for cid in chats_list[:10])
            if len(chats_list) > 10:
                txt += f"\n  ... и ещё {len(chats_list)-10}"
        txt += "\n\n"
        if dm_cfg.get("text"):
            txt += f"✉️ <b>Текст ЛС:</b>\n<i>{dm_cfg['text'][:100]}</i>"
        else:
            txt += "❌ Текст для ЛС не задан"
        if running:
            txt += "\n\n🟡 <b>Рассылка в процессе...</b>"
        await m.answer(txt, parse_mode="HTML", reply_markup=kb_dm())
        return

    if t == B["dm_text"]:
        set_wait(m, "dm_text")
        cur = dm_cfg.get("text", "")
        await m.answer(
            f"✏️ Введи текст для личных сообщений:\n\n"
            f"{'Текущий: <i>' + cur[:100] + '</i>' if cur else 'Текст не задан'}",
            parse_mode="HTML",
        )
        return

    if t == B["dm_chat"]:
        set_wait(m, "dm_chat")
        await m.answer(
            "📌 Введи ID чата, @username или ссылку:\n\n"
            "Примеры:\n"
            "  <code>-1001234567890</code>\n"
            "  <code>@mygroup</code>\n"
            "  <code>https://t.me/mygroup</code>",
            parse_mode="HTML",
        )
        return

    if t == B["dm_clr"]:
        dm_cfg["chats"] = []
        sv(F["dm"], dm_cfg)
        await m.answer("✅ Список чатов очищен")
        return

    if t == B["dm_top"]:
        set_wait(m, "dm_top")
        await m.answer(
            f"🔢 Сколько самых активных участников брать?\n"
            f"Текущее: {dm_cfg.get('top_n', 10)}\nВведи число (1–200):"
        )
        return

    if t == B["dm_lim"]:
        set_wait(m, "dm_lim")
        await m.answer(
            f"📈 Сколько последних сообщений анализировать?\n"
            f"Текущее: {dm_cfg.get('scan_limit', 500)}\nВведи число (10–5000):"
        )
        return

    if t == B["dm_prev"]:
        # Предпросмотр — показать топ без отправки ЛС
        aa = active()
        if not aa:
            await m.answer("❌ Нет активных аккаунтов!")
            return
        chat_ids = dm_cfg.get("chats", [])
        if not chat_ids:
            await m.answer("❌ Сначала добавь чаты!")
            return
        limit = int(dm_cfg.get("scan_limit", 500))
        top_n = int(dm_cfg.get("top_n", 10))
        await m.answer(f"🔍 Анализирую {len(chat_ids)} чат(ов)...")
        client = clients[aa[0]["phone"]]
        all_users: dict = {}
        for cid in chat_ids:
            try:
                users = await get_active_users(client, int(cid), limit)
                for u in users:
                    uid2 = u["user_id"]
                    if uid2 in all_users:
                        all_users[uid2]["count"] += u["count"]
                    else:
                        all_users[uid2] = dict(u)
            except Exception as e:
                log(f"preview err {cid}: {e}")
        if not all_users:
            await m.answer("😔 Участников не найдено")
            return
        sorted_u = sorted(all_users.values(), key=lambda x: x["count"], reverse=True)[:top_n]
        preview = "\n".join(
            f"  #{i+1} {u['name']} (@{u['username'] or '—'}) — {u['count']} сообщ."
            for i, u in enumerate(sorted_u)
        )
        await m.answer(
            f"📊 <b>Топ {len(sorted_u)} активных участников:</b>\n\n{preview}",
            parse_mode="HTML",
        )
        return

    if t == B["dm_go"]:
        if dm_cfg.get("running"):
            await m.answer("🟡 Рассылка уже идёт!")
            return
        if not dm_cfg.get("text"):
            await m.answer("❌ Сначала задай текст кнопкой «✏️ Текст для ЛС»!")
            return
        if not dm_cfg.get("chats"):
            await m.answer("❌ Сначала добавь чаты кнопкой «📌 Добавить чат»!")
            return
        if not active():
            await m.answer("❌ Нет активных аккаунтов!")
            return
        S["dm_task"] = asyncio.create_task(dm_task_run(int(uid)))
        await m.answer(
            "🚀 Запущена рассылка в личные сообщения!\n"
            "Я сообщу о результатах по завершении.",
            reply_markup=kb_dm(),
        )
        return

    if t == B["dm_stop"]:
        dm_cfg["running"] = False
        sv(F["dm"], dm_cfg)
        if S.get("dm_task"):
            S["dm_task"].cancel()
            S["dm_task"] = None
        await m.answer("🛑 Рассылка в ЛС остановлена", reply_markup=kb_dm())
        return

    if t == B["dm_base"]:
        total = len(dm_sent)
        if total == 0:
            await m.answer("📂 База пуста — ещё никому не писали.")
            return
        # Последние 15 отправленных
        items = list(dm_sent.items())[-15:]
        lines = []
        for uid_str, info in reversed(items):
            status = "✅" if info.get("ok") else "❌"
            tag = f"@{info['username']}" if info.get("username") else info.get("name", uid_str)
            reason = f" ({info['reason']})" if not info.get("ok") and info.get("reason") else ""
            lines.append(f"{status} {tag} — {info.get('date','?')}{reason}")
        await m.answer(
            f"<b>📂 База отправленных: {total} чел.</b>\n\n"
            f"<i>Последние 15:</i>\n" + "\n".join(lines) + "\n\n"
            f"ℹ️ Нажми <b>♻️ Сбросить базу</b> чтобы начать заново с нуля.",
            parse_mode="HTML",
        )
        return

    if t == B["dm_reset"]:
        count = len(dm_sent)
        dm_sent.clear()
        sv(F["sent_dm"], dm_sent)
        await m.answer(
            f"♻️ <b>База сброшена!</b>\n"
            f"Удалено записей: {count}\n\n"
            f"Теперь бот будет писать всем заново — как будто впервые.",
            parse_mode="HTML",
        )
        return

    if t == B["dm_stat"]:
        h = dm_stat.get("log", [])[-5:]
        ht = "\n".join(
            f"  {x['d']} ✅{x['sent']} ❌{x['fail']} ⛔{x.get('skipped',0)} | {x['chats']} чатов, топ {x['top_n']}"
            for x in reversed(h)
        ) or "  —"
        sent_ok = sum(1 for v in dm_sent.values() if v.get("ok"))
        sent_fail = len(dm_sent) - sent_ok
        await m.answer(
            f"<b>📊 Статистика ЛС-рассылки</b>\n\n"
            f"<b>📂 База обработанных:</b> {len(dm_sent)} чел.\n"
            f"  ✅ Успешно отправлено: {sent_ok}\n"
            f"  ⛔ Пропущено (ЛС уже открыт): {sent_fail}\n\n"
            f"<b>Всего за всё время:</b>\n"
            f"  📤 Отправлено: {dm_stat['sent']}\n"
            f"  ❌ Ошибок: {dm_stat['fail']}\n"
            f"🕐 Последний запуск: {dm_stat.get('last_run', '—')}\n\n"
            f"<b>История запусков:</b>\n{ht}",
            parse_mode="HTML",
        )
        return

    # ══════════════════════════════════════════════════════════
    # ███  ОБРАБОТЧИКИ МОДУЛЯ «МАССОВАЯ РАССЫЛКА»  ███
    # ══════════════════════════════════════════════════════════

    if t == B["mass"]:
        set_wait(m, "")
        running = mass_cfg.get("running", False)
        users_count = len(mass_cfg.get("users", []))
        use_base = mass_cfg.get("use_dm_sent", True)
        mass_text = mass_cfg.get("text", "").strip()
        
        status_icon = "🟢 ЗАПУЩЕНА" if running else "🔴 ОСТАНОВЛЕНА"
        txt = (
            f"<b>📤 Массовая рассылка по базе</b>\n\n"
            f"Статус: <b>{status_icon}</b>\n\n"
            f"📋 Загружено пользователей: <b>{users_count}</b>\n"
            f"⏳ Задержка: {mass_cfg.get('delay', 2.0)}с\n"
            f"🔗 Общая база: {'✅' if use_base else '❌'}\n\n"
        )
        
        if mass_text:
            txt += f"✉️ <b>Текст:</b>\n<i>{mass_text[:120]}</i>"
        else:
            txt += "❌ Текст не задан"
        
        if running:
            s = mass_stat
            txt += (
                f"\n\n📊 <b>Текущая сессия:</b>\n"
                f"  ✅ Отправлено: {s['sent']}\n"
                f"  ⏭ Пропущено: {s['skip']}\n"
                f"  ❌ Ошибок: {s['fail']}"
            )
        
        await m.answer(txt, parse_mode="HTML", reply_markup=kb_mass())
        return

    if t == B["mass_load"]:
        set_wait(m, "mass_load")
        await m.answer(
            "📁 <b>Загрузить базу пользователей</b>\n\n"
            "Отправь:\n"
            "• Текстовый файл (.txt)\n"
            "• Или просто текст списком\n\n"
            "<b>Формат (каждый с новой строки):</b>\n"
            "<code>@username\n"
            "username\n"
            "123456789\n"
            "https://t.me/username</code>\n\n"
            "ℹ️ Строки начинающиеся с # игнорируются (комментарии)",
            parse_mode="HTML"
        )
        return

    if t == B["mass_text"]:
        set_wait(m, "mass_text")
        cur = mass_cfg.get("text", "")
        await m.answer(
            f"✏️ Введи текст для массовой рассылки:\n\n"
            f"{'Текущий: <i>' + cur[:100] + '</i>' if cur else 'Текст не задан'}",
            parse_mode="HTML"
        )
        return

    if t == B["mass_view"]:
        users = mass_cfg.get("users", [])
        if not users:
            await m.answer("📭 База пуста")
            return
        preview = "\n".join(f"  {i+1}. {u}" for i, u in enumerate(users[:20]))
        if len(users) > 20:
            preview += f"\n  ... и ещё {len(users)-20}"
        await m.answer(f"<b>📋 База ({len(users)} польз.):</b>\n\n{preview}", parse_mode="HTML")
        return

    if t == B["mass_clr"]:
        cnt = len(mass_cfg.get("users", []))
        mass_cfg["users"] = []
        sv(F["mass"], mass_cfg)
        # Очищаем файл
        if F["mass_users"].exists():
            F["mass_users"].unlink()
        await m.answer(f"✅ База очищена ({cnt} польз.)")
        return

    if t == B["mass_base"]:
        mass_cfg["use_dm_sent"] = not mass_cfg.get("use_dm_sent", True)
        sv(F["mass"], mass_cfg)
        st = "✅ включена" if mass_cfg["use_dm_sent"] else "❌ выключена"
        await m.answer(
            f"🔗 Общая база {st}\n\n"
            f"{'Не будет писать тем кому уже писали' if mass_cfg['use_dm_sent'] else 'Будет писать всем подряд'}"
        )
        return

    if t == B["mass_go"]:
        if mass_cfg.get("running"):
            await m.answer("🟢 Рассылка уже идёт!")
            return
        if not mass_cfg.get("users"):
            await m.answer("❌ Сначала загрузи базу (📁 Загрузить базу)!")
            return
        if not mass_cfg.get("text"):
            await m.answer("❌ Задай текст (✏️ Текст)!")
            return
        if not active():
            await m.answer("❌ Нет активных аккаунтов!")
            return
        
        mass_stat.update({"sent": 0, "fail": 0, "skip": 0})
        S["mass_task"] = asyncio.create_task(mass_send_task(int(uid)))
        await m.answer(
            "🚀 Массовая рассылка запущена!\n"
            "Я сообщу о результатах по завершении.",
            reply_markup=kb_mass()
        )
        return

    if t == B["mass_stop"]:
        mass_cfg["running"] = False
        sv(F["mass"], mass_cfg)
        if S.get("mass_task"):
            S["mass_task"].cancel()
            S["mass_task"] = None
        await m.answer("🛑 Рассылка остановлена", reply_markup=kb_mass())
        return

    if t == B["mass_stat"]:
        s = mass_stat
        running_str = "🟢 работает" if mass_cfg.get("running") else "🔴 остановлена"
        users_count = len(mass_cfg.get("users", []))
        await m.answer(
            f"<b>📊 Статистика массовой рассылки</b>\n\n"
            f"Статус: {running_str}\n"
            f"Загружено пользователей: {users_count}\n\n"
            f"<b>Всего за всё время:</b>\n"
            f"  ✅ Отправлено: {s['sent']}\n"
            f"  ⏭ Пропущено: {s['skip']}\n"
            f"  ❌ Ошибок: {s['fail']}\n"
            f"🕐 Последний запуск: {s.get('last_run', '—')}\n\n"
            f"📂 В общей базе: {len(dm_sent)} чел.",
            parse_mode="HTML"
        )
        return

    # ══════════════════════════════════════════════════════════
    # ███  ОБРАБОТЧИКИ МОДУЛЯ «МОНИТОРИНГ»  ███
    # ══════════════════════════════════════════════════════════

    if t == B["mon"]:
        set_wait(m, "")
        active_mon  = monitor_cfg.get("active", False)
        chats_list  = monitor_cfg.get("chats", [])
        use_base    = monitor_cfg.get("use_dm_sent", True)
        mon_text    = monitor_cfg.get("text", "").strip()
        fallback    = "(берётся из модуля «Активные → ЛС»)" if not mon_text else ""

        status_icon = "🟢 ВКЛЮЧЁН" if active_mon else "🔴 ВЫКЛЮЧЕН"
        txt = (
            f"<b>👁 Мониторинг в реальном времени</b>\n\n"
            f"Статус: <b>{status_icon}</b>\n\n"
            f"📌 Отслеживаемых чатов: <b>{len(chats_list)}</b>\n"
        )
        if chats_list:
            txt += "\n".join(f"  • {c}" for c in chats_list[:8])
            if len(chats_list) > 8:
                txt += f"\n  ... и ещё {len(chats_list)-8}"
            txt += "\n"
        txt += (
            f"\n🔗 Общая база (не писать повторно): {'✅' if use_base else '❌'}\n"
            f"⏳ Задержка ответа: {monitor_cfg.get('delay', 1.5)}с\n\n"
            f"✉️ <b>Текст ЛС:</b>\n"
        )
        if mon_text:
            txt += f"<i>{mon_text[:120]}</i>"
        else:
            txt += f"<i>не задан — {fallback}</i>"

        if active_mon:
            s = monitor_stat
            txt += (
                f"\n\n📊 <b>С момента включения:</b>\n"
                f"  👁 Событий: {s['events']}\n"
                f"  ✅ Отправлено: {s['sent']}\n"
                f"  ⏭ Пропущено (уже писали): {s['skip']}\n"
                f"  ❌ Ошибок: {s['fail']}"
            )
        await m.answer(txt, parse_mode="HTML", reply_markup=kb_monitor())
        return

    if t == B["mon_text"]:
        set_wait(m, "mon_text")
        cur = monitor_cfg.get("text", "").strip()
        await m.answer(
            f"✏️ Введи текст для ЛС (мониторинг):\n\n"
            f"{'Текущий: <i>' + cur[:100] + '</i>' if cur else 'Если оставить пустым — возьмётся текст из модуля «Активные → ЛС»'}\n\n"
            f"Введи текст или <code>-</code> чтобы очистить (использовать общий):",
            parse_mode="HTML",
        )
        return

    if t == B["mon_chat"]:
        set_wait(m, "mon_chat")
        await m.answer(
            "📌 Введи ID чата, @username или ссылку для <b>мониторинга</b>:\n\n"
            "Примеры:\n"
            "  <code>-1001234567890</code>\n"
            "  <code>@mygroup</code>\n"
            "  <code>https://t.me/mygroup</code>\n\n"
            "⚠️ Аккаунт должен быть участником этого чата!",
            parse_mode="HTML",
        )
        return

    if t == B["mon_clr"]:
        cnt = len(monitor_cfg["chats"])
        monitor_cfg["chats"] = []
        sv(F["monitor"], monitor_cfg)
        # Если мониторинг был включён — перезапускаем без чатов
        if monitor_cfg.get("active"):
            monitor_stop()
            monitor_cfg["active"] = False
            sv(F["monitor"], monitor_cfg)
        await m.answer(f"✅ Очищено {cnt} чатов мониторинга")
        return

    if t == B["mon_base"]:
        monitor_cfg["use_dm_sent"] = not monitor_cfg.get("use_dm_sent", True)
        sv(F["monitor"], monitor_cfg)
        st = "✅ включена" if monitor_cfg["use_dm_sent"] else "❌ выключена"
        await m.answer(
            f"🔗 Общая база {st}\n\n"
            f"{'Мониторинг не будет писать тем кому уже писали через модуль «Активные → ЛС»' if monitor_cfg['use_dm_sent'] else 'Мониторинг будет писать всем подряд (игнорирует базу)'}"
        )
        return

    if t == B["mon_on"]:
        if monitor_cfg.get("active"):
            await m.answer("👁 Мониторинг уже включён!")
            return
        if not monitor_cfg.get("chats"):
            await m.answer("❌ Сначала добавь чаты кнопкой «📌 Добавить чат (мон.)»!")
            return
        # Проверяем текст
        text_ok = monitor_cfg.get("text", "").strip() or dm_cfg.get("text", "").strip()
        if not text_ok:
            await m.answer("❌ Задай текст для ЛС (кнопка «✏️ Текст (мон.)» или в модуле «Активные → ЛС»)!")
            return
        if not active():
            await m.answer("❌ Нет активных аккаунтов!")
            return

        # Сбрасываем счётчики
        monitor_stat.update({"sent": 0, "skip": 0, "fail": 0, "events": 0})

        # Получаем список uid для уведомлений
        notify_list = list(S.get("notify_users", set()))

        ok = await monitor_start(notify_list)
        if ok:
            monitor_cfg["active"] = True
            sv(F["monitor"], monitor_cfg)
            chats_count = len(monitor_cfg["chats"])
            await m.answer(
                f"👁 <b>Мониторинг ВКЛЮЧЁН</b>\n\n"
                f"Слежу за {chats_count} чатом(-ами).\n"
                f"Как только кто-то напишет — сразу отправлю ему ЛС.\n"
                f"Повторно писать не буду {'(общая база активна)' if monitor_cfg.get('use_dm_sent') else '(база отключена)'}.",
                parse_mode="HTML",
                reply_markup=kb_monitor(),
            )
        else:
            await m.answer("❌ Не удалось запустить мониторинг. Проверь аккаунты и чаты.")
        return

    if t == B["mon_off"]:
        monitor_cfg["active"] = False
        sv(F["monitor"], monitor_cfg)
        monitor_stop()
        s = monitor_stat
        await m.answer(
            f"⏹ <b>Мониторинг ОСТАНОВЛЕН</b>\n\n"
            f"📊 За сессию:\n"
            f"  👁 Событий замечено: {s['events']}\n"
            f"  ✅ Отправлено ЛС: {s['sent']}\n"
            f"  ⏭ Пропущено: {s['skip']}\n"
            f"  ❌ Ошибок: {s['fail']}",
            parse_mode="HTML",
            reply_markup=kb_monitor(),
        )
        return

    if t == B["mon_stat"]:
        s = monitor_stat
        active_str = "🟢 работает" if monitor_cfg.get("active") else "🔴 остановлен"
        chats = monitor_cfg.get("chats", [])
        await m.answer(
            f"<b>📊 Статистика мониторинга</b>\n\n"
            f"Статус: {active_str}\n"
            f"Чатов: {len(chats)}\n\n"
            f"<b>Текущая сессия:</b>\n"
            f"  👁 Событий: {s['events']}\n"
            f"  ✅ Отправлено: {s['sent']}\n"
            f"  ⏭ Пропущено (уже писали): {s['skip']}\n"
            f"  ❌ Не доставлено: {s['fail']}\n\n"
            f"📂 В общей базе: {len(dm_sent)} чел.",
            parse_mode="HTML",
        )
        return

    # По умолчанию — главное меню
    await show_main(m)


# ══════════════ ЗАПУСК ══════════════
async def main():
    print("=" * 50)
    print("  PRO v16.0 — БЕЗ АВТОРИЗАЦИИ")
    print("  + Активные участники → ЛС")
    print("  + База отправленных (без повторов)")
    print("  + 👁 Живой мониторинг в реальном времени")
    print("  + 📤 Массовая рассылка по файлу/списку")
    print("=" * 50)
    if accs:
        ok = await conn_all()
        print(f"✅ Подключено аккаунтов: {ok}/{len(accs)}")
    if targets:
        print(f"📋 Чатов в базе: {len(targets)}")
        rebuild_list()

    # Восстанавливаем мониторинг если был активен до перезапуска
    if monitor_cfg.get("active") and monitor_cfg.get("chats") and active():
        ok = await monitor_start([])
        if ok:
            print(f"👁 Мониторинг восстановлен: {len(monitor_cfg['chats'])} чатов")
        else:
            monitor_cfg["active"] = False
            sv(F["monitor"], monitor_cfg)
            print("⚠️ Не удалось восстановить мониторинг")

    await bot.set_my_commands([
        BotCommand(command="start", description="📨 Главное меню"),
    ])
    print("🤖 Бот запущен. Напиши /start")
    print("=" * 50)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
