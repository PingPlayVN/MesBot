"""
fbchat-v2 — Minimal bot
=================================================

Bot này minh hoạ cách kết hợp 3 tầng `_core` / `_features` / `_messaging`
để tạo một con bot chat đơn giản phản hồi lệnh trong nhóm hoặc DM.

This bot demonstrates how to combine the three layers
(`_core` / `_features` / `_messaging`) into a small command-driven bot.

Lệnh hỗ trợ / Supported commands:
    /ping              -> trả lời "pong" (latency check)
    /help              -> hiển thị danh sách lệnh
    /id                -> in threadID + userID của người gửi
    /echo <text>       -> lặp lại nội dung
    /search <keyword>  -> tìm người dùng Facebook
    /unsend            -> thu hồi tin nhắn cuối của bot trong thread

Cấu hình / Configuration:
    Tạo file `config.json` cùng thư mục với main.py:
        {
            "cookies": "c_user=...; xs=...; fr=...; datr=...;",
            "prefix":  "/",
            "admins":  ["1000xxxxxxxxxx"]
        }

@MinhHuyDev (Claude Opus 4.7) | Telegram: @minhhuydev
"""

from __future__ import annotations

import json
import os
import sys
import time
import threading
import traceback
import random
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Bảo đảm `src/` nằm trong sys.path khi chạy file này trực tiếp
# Ensure `src/` is on sys.path when running this file directly
# ---------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from _core._session import dataGetHome
from _features._facebook import _search, _get_user_info
from _messaging._send import api as SendAPI
from _messaging._unsend import func as unsend_message
from _messaging._listening import listeningEvent
from keep_alive import keep_alive


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

CONFIG_PATH = HERE / "config.json"


def load_config() -> dict:
    """Đọc config.json. Tạo template nếu chưa tồn tại."""
    if not CONFIG_PATH.exists():
        template = {
            "cookies": "PASTE_YOUR_FACEBOOK_COOKIE_HERE",
            "prefix": "/",
            "admins": [],
        }
        CONFIG_PATH.write_text(
            json.dumps(template, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"[config] Đã tạo template tại {CONFIG_PATH}. "
              "Hãy điền 'cookies' rồi chạy lại.")
        sys.exit(1)

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    if not cfg.get("cookies") or "PASTE_YOUR" in cfg["cookies"]:
        print("[config] Bạn chưa điền cookie Facebook trong config.json.")
        sys.exit(1)

    cfg.setdefault("prefix", "/")
    cfg.setdefault("admins", [])
    return cfg


def is_valid_datafb(dataFB: object) -> bool:
    if not isinstance(dataFB, dict):
        return False

    facebook_id = str(dataFB.get("FacebookID") or "").strip()
    if not facebook_id.isdigit():
        return False

    required_fields = ("fb_dtsg", "jazoest", "sessionID", "clientRevision", "cookieFacebook")
    return all(str(dataFB.get(field) or "").strip() for field in required_fields)


def log(tag: str, msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] [{tag}] {msg}")


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class SimpleBot:
    """Bot tối giản — poll `listener.bodyResults` và phản hồi theo lệnh."""

    def __init__(self, dataFB: dict, prefix: str = "/", admins: list | None = None):
        self.dataFB = dataFB
        self.prefix = prefix
        self.admins = set(map(str, admins or []))

        self.sender = SendAPI()
        self.listener = listeningEvent(dataFB)

        # Theo dõi messageID đã xử lý → tránh phản hồi 2 lần cùng 1 tin
        self._last_seen_message_id: str | None = None
        # Lưu messageID cuối cùng bot đã gửi vào mỗi thread (cho /unsend)
        self._last_bot_message: dict[str, str] = {}

        self._games = {}

        # Map prefix-less command -> handler
        self._handlers = {
            "ping":   self._cmd_ping,
            "help":   self._cmd_help,
            "id":     self._cmd_id,
            "echo":   self._cmd_echo,
            "search": self._cmd_search,
            "unsend": self._cmd_unsend,
            "hibot":  self._cmd_hibot,
            "doanso": self._cmd_doanso,
            "huygame": self._cmd_huygame,
            "noitu":  self._cmd_noitu,
        }

    # -- public ---------------------------------------------------------------

    def run(self) -> None:
        """Khởi động listener trong thread riêng và poll sự kiện."""
        log("bot", f"Đăng nhập với UID = {self.dataFB.get('FacebookID')}")
        self.listener.get_last_seq_id()

        # `connect_mqtt()` là blocking (loop_forever) → chạy trong thread daemon
        t = threading.Thread(
            target=self.listener.connect_mqtt,
            name="fbchat-listener",
            daemon=True,
        )
        t.start()
        log("bot", "Listener đã khởi động. Nhấn Ctrl+C để thoát.")

        try:
            while True:
                self._poll_once()
                time.sleep(0.3)
        except KeyboardInterrupt:
            log("bot", "Đã dừng theo yêu cầu người dùng.")

    # -- internal -------------------------------------------------------------

    def _poll_once(self) -> None:
        """Quét bodyResults; nếu có tin mới chưa xử lý → dispatch."""
        get_message = getattr(self.listener, "get_message", None)
        snap = get_message() if callable(get_message) else self.listener.bodyResults
        if snap is None:
            return
        mid = snap.get("messageID")
        body = snap.get("body")

        if not mid or mid == self._last_seen_message_id:
            return
        self._last_seen_message_id = mid

        # Bỏ qua tin do chính bot gửi
        sender_id = str(snap.get("userID") or "")
        if sender_id == str(self.dataFB.get("FacebookID")):
            return

        if not body:
            return

        log("recv", f"[{snap.get('type')}] {sender_id}@{snap.get('replyToID')}: {body!r}")

        if not body.startswith(self.prefix):
            # Xử lý các trò chơi đang diễn ra (không cần dấu /)
            thread_id = str(snap.get("replyToID"))
            if thread_id in self._games:
                game = self._games[thread_id]
                text = body.strip().lower()
                
                # 1. Nếu là game Đoán số (game lưu dạng số nguyên)
                if isinstance(game, int) and text.isdigit():
                    self._handle_guess(snap, thread_id, int(text))
                
                # 2. Nếu là game Nối từ (game lưu dạng từ điển dict)
                elif isinstance(game, dict) and game.get("type") == "noitu":
                    # Chỉ kiểm tra nếu tin nhắn có đúng 2 chữ (ngăn bot rep bừa khi mọi người đang chat)
                    if len(text.split()) == 2:
                        self._handle_noitu(snap, thread_id, text)
            return

        # Tách lệnh
        without_prefix = body[len(self.prefix):].strip()
        if not without_prefix:
            return
        parts = without_prefix.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        handler = self._handlers.get(cmd)
        if handler is None:
            return  # im lặng cho lệnh không biết

        try:
            handler(snap, arg)
        except Exception as exc:  # noqa: BLE001 - tránh crash listener thread
            log("err", f"Lỗi khi xử lý lệnh /{cmd}: {exc}")
            traceback.print_exc()

    # -- send wrapper ---------------------------------------------------------

    def _reply(self, snap: dict, content: str) -> None:
        thread_id = snap["replyToID"]
        type_chat = "user" if snap.get("type") == "user" else None

        # [THÊM MỚI] LÀM CHẬM TIN NHẮN ĐỂ TRÁNH BỊ FB QUÉT SPAM
        # Tạo delay ngẫu nhiên từ 1.0 đến 1.5 giây (1000ms - 1500ms)
        delay_time = random.uniform(1.5, 3)
        
        # In ra màn hình để bạn dễ theo dõi
        log("delay", f"Đang chờ {delay_time:.2f}s để giả lập người thật...")
        
        # Lệnh sleep sẽ chặn bot lại, đợi đủ thời gian mới chạy tiếp
        time.sleep(delay_time)

        result = self.sender.send(
            self.dataFB,
            content,
            thread_id,
            typeChat=type_chat,
            replyMessage=True,
            messageID=snap.get("messageID"),
        )

        if isinstance(result, dict) and result.get("success") == 1:
            try:
                self._last_bot_message[str(thread_id)] = (
                    result["payload"]["messageID"]
                )
            except (KeyError, TypeError):
                pass
            log("send", f"-> {thread_id}: {content!r}")
        else:
            log("send", f"FAIL -> {thread_id}: {result}")

    # -- commands -------------------------------------------------------------

    def _cmd_ping(self, snap: dict, arg: str) -> None:
        sent_ts = int(snap.get("timestamp") or 0)
        if sent_ts:
            latency_ms = max(0, int(time.time() * 1000) - sent_ts)
            self._reply(snap, f"🏓 pong! ({latency_ms} ms)")
        else:
            self._reply(snap, "🏓 pong!")

    def _cmd_help(self, snap: dict, arg: str) -> None:
        p = self.prefix
        self._reply(snap, (
            "📖 Lệnh hỗ trợ:\n"
            f"• {p}ping — kiểm tra độ trễ\n"
            f"• {p}help — hiển thị trợ giúp\n"
            f"• {p}id — xem threadID + userID\n"
            f"• {p}echo <text> — lặp lại nội dung\n"
            f"• {p}search <từ> — tìm user Facebook\n"
            f"• {p}unsend — thu hồi tin nhắn cuối của bot\n"
            f"• {p}hibot — lời chào thân thiện\n"
            f"• {p}doanso — minigame đoán số từ 1 đến 100\n"
            f"• {p}huygame — hủy trò chơi"
        ))

    def _cmd_id(self, snap: dict, arg: str) -> None:
        self._reply(snap, (
            f"🆔 type      : {snap.get('type')}\n"
            f"   threadID  : {snap.get('replyToID')}\n"
            f"   userID    : {snap.get('userID')}\n"
            f"   messageID : {snap.get('messageID')}"
        ))

    def _cmd_echo(self, snap: dict, arg: str) -> None:
        if not arg:
            self._reply(snap, f"Cách dùng: {self.prefix}echo <nội dung>")
            return
        self._reply(snap, arg)

    def _cmd_search(self, snap: dict, arg: str) -> None:
        if not arg:
            self._reply(snap, f"Cách dùng: {self.prefix}search <từ khoá>")
            return
        try:
            res = _search.func(self.dataFB, arg)
        except Exception as exc:  # noqa: BLE001
            self._reply(snap, f"❌ Lỗi tìm kiếm: {exc}")
            return

        users = res.get("searchResultsDict") if isinstance(res, dict) else None
        if not users:
            self._reply(snap, f"🔍 Không tìm thấy kết quả nào cho: {arg}")
            return

        lines = [f"🔍 Kết quả cho “{arg}”:"]
        for i, u in enumerate(users[:5], 1):
            lines.append(f"{i}. {u.get('name')} — {u.get('id')}")
        self._reply(snap, "\n".join(lines))

    def _cmd_unsend(self, snap: dict, arg: str) -> None:
        # Chỉ admin mới được dùng nếu có cấu hình admins
        sender_id = str(snap.get("userID") or "")
        if self.admins and sender_id not in self.admins:
            self._reply(snap, "⛔ Chỉ admin mới được dùng lệnh này.")
            return

        thread_id = str(snap["replyToID"])
        target = self._last_bot_message.get(thread_id)
        if not target:
            self._reply(snap, "ℹ️ Chưa có tin nào để thu hồi trong thread này.")
            return

        result = unsend_message(target, self.dataFB)
        log("unsend", f"{target} -> {result}")
        # Sau khi thu hồi → quên ID đó
        self._last_bot_message.pop(thread_id, None)

    def _cmd_hibot(self, snap: dict, arg: str) -> None:
        import random # Thêm thư viện random vào đây để trộn tin nhắn

        # Lấy ID Facebook của người gửi tin nhắn
        user_id = str(snap.get("userID") or "")
        user_name = "bạn"
        
        try:
            res = _get_user_info.func(self.dataFB, user_id)
            if isinstance(res, dict) and "err" not in res:
                user_name = res.get("nameUser") or res.get("firstName") or "bạn"
        except Exception as exc:
            log("err", f"Không thể lấy tên user: {exc}")
            
        # Bỏ tất cả các câu chào bạn muốn vào một danh sách (List)
        danh_sach_chao = [
            f"Hi @{user_name} mình là bạn thế quang ",
            f"👋 Chào @{user_name}! Tôi không biết đọc suy nghĩ đâu, gõ lệnh đi. 😭",
            f"🤖 Xin chào @{user_name}! Tín hiệu ổn định. Não của tôi cũng tạm ổn",
            "🐸 Hello! Tôi là bot, không phải Google nên đừng hỏi 'bạn khỏe không'.",
            "😎 Yo! Tôi là bot, đẹp trai nhất trong đoạn chat này."
        ]
        
        # Lệnh random.choice sẽ bốc thăm ngẫu nhiên 1 câu trong danh sách trên
        cau_tra_loi_ngau_nhien = random.choice(danh_sach_chao)
            
        # Trả lời lại vào nhóm chat với câu đã bốc thăm được
        self._reply(snap, cau_tra_loi_ngau_nhien)

    def _cmd_doanso(self, snap: dict, arg: str) -> None:
        import random
        thread_id = str(snap["replyToID"])
        
        # Kiểm tra xem nhóm có đang chơi dở ván nào không
        if thread_id in self._games:
            self._reply(snap, "⚠️ Nhóm đang có ván game chưa kết thúc! Hãy đoán số hoặc gõ /huygame để chơi ván mới.")
            return
        
        # Bot random số từ 1 đến 100 và lưu lại
        so_bi_mat = random.randint(1, 100)
        self._games[thread_id] = so_bi_mat
        
        loi_chao = (
            "🎮 TRÒ CHƠI ĐOÁN SỐ BẮT ĐẦU!\n"
            "Bot đã nghĩ ra một số từ 1 đến 100.\n"
            "Cả nhóm hãy thi nhau gõ thẳng một con số vào nhóm để đoán nhé! (Ví dụ nhắn: 50)"
        )
        self._reply(snap, loi_chao)

    def _cmd_huygame(self, snap: dict, arg: str) -> None:
        thread_id = str(snap["replyToID"])
        if thread_id in self._games:
            game_data = self._games.pop(thread_id)
            if isinstance(game_data, int): # Của trò đoán số
                self._reply(snap, f"🛑 Đã hủy game Đoán số. Đáp án đúng là: {game_data}")
            else: # Của trò nối từ
                self._reply(snap, "🛑 Đã kết thúc trò chơi Nối từ. Cả nhóm chat bình thường nhé!")
        else:
            self._reply(snap, "Hiện tại không có ván game nào để hủy.")

    def _cmd_noitu(self, snap: dict, arg: str) -> None:
        import random
        thread_id = str(snap["replyToID"])
        
        if thread_id in self._games:
            self._reply(snap, "⚠️ Nhóm đang có ván game chưa xong! Hãy gõ /huygame nếu muốn chơi ván mới.")
            return
        
        kho_tu = ["con mèo", "hoa hồng", "bầu trời", "máy tính", "gia đình", "tình yêu", "bạn bè"]
        tu_khoi_dau = random.choice(kho_tu)
        chu_cuoi = tu_khoi_dau.split()[1]
        
        # LƯU THÊM 2 BIẾN TRẠNG THÁI MỚI VÀO GAME
        self._games[thread_id] = {
            "type": "noitu",
            "last_char": chu_cuoi,
            "used": [tu_khoi_dau],
            "last_user_id": None,  # Để nhớ xem ai vừa chơi
            "is_checking": False   # Cờ khóa: Đánh dấu đang bận tra từ điển
        }
        
        luat_choi = (
            "🔤 TRÒ CHƠI NỐI TỪ BẮT ĐẦU!\n"
            "Luật 1: Mỗi người 1 lượt, không được nối 2 lần liên tiếp.\n"
            "Luật 2: Đợi Trọng tài check xong mới được nối tiếp.\n\n"
            f"Từ khởi đầu của Bot: {tu_khoi_dau.upper()}\n"
            f"👉 Tiếp theo bắt đầu bằng chữ: '{chu_cuoi.upper()}'"
        )
        self._reply(snap, luat_choi)

    def _handle_noitu(self, snap: dict, thread_id: str, text: str) -> None:
        import requests
        
        game = self._games[thread_id]
        user_id = str(snap.get("userID") or "")
        
        # ====================================================
        # LUẬT 1: CHỜ BOT CHECK XONG (KHÓA ĐỒNG BỘ)
        # Nếu bot đang bận lên mạng tra từ điển thì bỏ qua mọi tin nhắn khác
        if game.get("is_checking") == True:
            return 
            
        # LUẬT 2: KHÔNG ĐƯỢC CHƠI 2 LƯỢT LIÊN TIẾP
        if user_id == game.get("last_user_id"):
            self._reply(snap, "🚫 Tham lam! Bạn vừa nối rồi, hãy nhường cơ hội cho người khác đi.")
            return
        # ====================================================

        words = text.split()
        
        # 0. BỘ LỌC TỪ BẬY
        danh_sach_den = ["cặc", "lồn", "đụ", "đĩ", "buồi", "dái", "cứt", "phò", "nứng", "địt"]
        for tu_bay in danh_sach_den:
            if tu_bay in text:
                self._reply(snap, f"🤬 Thẻ đỏ! Dùng từ thô tục '{text.upper()}' nha! Nhập lại từ khác đi.")
                return

        # 1. Kiểm tra luật nối chữ
        if words[0] != game["last_char"]:
            self._reply(snap, f"❌ Sai rồi! Phải bắt đầu bằng chữ '{game['last_char'].upper()}'.")
            return
            
        # 2. Kiểm tra từ trùng lặp
        if text in game["used"]:
            self._reply(snap, f"♻️ Từ '{text.upper()}' đã có người dùng rồi! Vui lòng nghĩ từ khác.")
            return
            
        # KHÓA GAME LẠI: Đánh dấu đang tra từ điển để chặn người khác xông vào
        game["is_checking"] = True
        
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        
        # 3. Kiểm tra từ có nghĩa
        try:
            url = f"https://vi.wiktionary.org/w/api.php?action=query&titles={text}&format=json"
            res = requests.get(url, headers=headers, timeout=5).json()
            pages = res.get("query", {}).get("pages", {})
            
            is_valid = True
            for page_id, info in pages.items():
                if int(page_id) < 0 or "missing" in info:
                    is_valid = False
                    
            if not is_valid:
                self._reply(snap, f"❓ Chữ '{text.upper()}' không có trong từ điển tiếng Việt! Bớt tự bịa ra đi nha.")
                game["is_checking"] = False # Nhả khóa ra cho nhập lại
                return
        except Exception as e:
            print(f"[Lỗi Từ Điển] {e}")
            self._reply(snap, f"⚠️ Trọng tài đang bị lỗi không tra được từ điển lúc này. Thử lại chữ khác nhé!")
            game["is_checking"] = False # Nhả khóa ra
            return
            
        # 4. Ghi nhận từ hợp lệ
        game["used"].append(text)
        game["last_char"] = words[1]
        game["last_user_id"] = user_id # Lưu ID của người vừa nối thành công
        
        # Lấy tên người chơi
        user_name = "Người chơi"
        try:
            res = _get_user_info.func(self.dataFB, str(snap.get("userID") or ""))
            if isinstance(res, dict) and "err" not in res:
                user_name = res.get("nameUser") or res.get("firstName") or "Bạn"
        except:
            pass

        # 5. KIỂM TRA ĐƯỜNG CÙNG (CƠ CHẾ KẾT THÚC GAME)
        try:
            check_url = f"https://vi.wiktionary.org/w/api.php?action=query&list=prefixsearch&pssearch={game['last_char']} &format=json"
            check_res = requests.get(check_url, headers=headers, timeout=5).json()
            search_results = check_res.get("query", {}).get("prefixsearch", [])
            
            has_continuation = False
            for item in search_results:
                title = item["title"].lower()
                if " " in title and title not in game["used"]:
                    has_continuation = True
                    break
                    
            if not has_continuation:
                self._games.pop(thread_id)
                msg_win = (
                    f"🏆 ĐỈNH CẤP!\n"
                    f"{user_name} vừa thả một từ 'chí mạng': 【 {text.upper()} 】\n\n"
                    f"Từ điển tiếng Việt đã bó tay, không còn từ nào có thể nối tiếp chữ '{game['last_char'].upper()}' nữa!\n"
                    f"🎉 CHÚC MỪNG {user_name.upper()} ĐÃ TRỞ THÀNH NGƯỜI CHIẾN THẮNG! 👑"
                )
                self._reply(snap, msg_win)
                # Game đã kết thúc (xóa khỏi bộ nhớ) nên không cần nhả khóa is_checking nữa
                return
                
        except Exception as e:
            print(f"[Lỗi Kiểm Tra End Game] {e}")

        # 6. MỞ KHÓA VÀ CHO CHƠI TIẾP
        self._reply(snap, f"✅ {user_name} nối chuẩn!\n👉 Chữ tiếp theo: '{game['last_char'].upper()}'...")
        game["is_checking"] = False # Nhả khóa ra để người khác nối

    def _handle_guess(self, snap: dict, thread_id: str, doan: int) -> None:
        so_bi_mat = self._games[thread_id]
        
        if doan == so_bi_mat:
            # Nếu đoán TRÚNG
            user_id = str(snap.get("userID") or "")
            user_name = "bạn"
            try:
                res = _get_user_info.func(self.dataFB, user_id)
                if isinstance(res, dict) and "err" not in res:
                    user_name = res.get("nameUser") or res.get("firstName") or "bạn"
            except:
                pass
                
            self._games.pop(thread_id) # Kết thúc game, xóa bộ nhớ
            self._reply(snap, f"🎉 CHÚC MỪNG {user_name.upper()} ĐÃ ĐOÁN TRÚNG!\n🏆 Đáp án chính xác là {so_bi_mat}.")
            
        elif doan < so_bi_mat:
            self._reply(snap, f"📈 Số {doan} bé quá, phải LỚN HƠN nữa!")
        else:
            self._reply(snap, f"📉 Số {doan} bự quá, phải NHỎ HƠN nữa!")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    cfg = load_config()

    log("boot", "Đang khởi tạo dataFB từ cookie…")
    dataFB = dataGetHome(cfg["cookies"])

    if not is_valid_datafb(dataFB):
        log("boot", "❌ Không lấy được dataFB hợp lệ — cookie có thể đã hết hạn hoặc HTML token đã đổi.")
        sys.exit(1)

    bot = SimpleBot(
        dataFB,
        prefix=cfg["prefix"],
        admins=cfg["admins"],
    )
    bot.run()


if __name__ == "__main__":
    keep_alive()
    main()
