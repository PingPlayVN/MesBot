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
            "nhac":   self._cmd_nhac,       # Đếm ngược (15m, 2h...)
            "hen":    self._cmd_hen,        # Hẹn giờ trong ngày (20:00)
            "lich":   self._cmd_lich,       # Hẹn ngày cụ thể (20:00 25/10)
            "ds_nhac": self._cmd_ds_nhac,   # Xem danh sách
            "huynhac": self._cmd_huynhac,
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

        t_reminder = threading.Thread(
            target=self._check_reminders_loop,
            name="firebase-reminders",
            daemon=True,
        )
        t_reminder.start()

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
        import random
        import time
        
        sent_ts = int(snap.get("timestamp") or 0)
        latency_ms = max(0, int(time.time() * 1000) - sent_ts) if sent_ts else 0
        
        # Danh sách các câu trả lời ngẫu nhiên
        danh_sach_ping = [
            f"🏓 Pong! Tốc độ bàn thờ: {latency_ms} ms nha.",
            f"Hệ thống vẫn sống nhăn răng! Độ trễ: {latency_ms} ms.",
            f"Ping cái gì mà ping, pong nè! ({latency_ms} ms)",
            f"Dạ có em! Đang lướt sóng với tốc độ {latency_ms} ms.",
            f"Lag quá lag quá... đùa tí, tốc độ là {latency_ms} ms nhé!"
        ]
        
        self._reply(snap, random.choice(danh_sach_ping))

    def _cmd_help(self, snap: dict, arg: str) -> None:
        import random
        p = self.prefix
        
        intros = [
            "📖 Chào đằng ấy, đây là bí kíp võ công của bot:\n",
            "🤖 Menu phục vụ của quán hôm nay gồm có:\n",
            "✨ Để tui liệt kê sương sương mấy tài lẻ của tui nha:\n",
            "💁‍♂️ Khách yêu cần gì cứ gọi theo cú pháp này nha:\n",
            "📜 Lấy sổ tay ra ghi chép lại các lệnh này nè:\n",
            "🛠 Trợ lý đa năng xin báo cáo danh sách tính năng:\n",
            "📂 Bảng vàng các lệnh bot hỗ trợ (đừng gõ sai nha):\n",
            "💡 Bí thuật giao tiếp với bot nằm hết ở đây:\n",
            "🧭 Bản đồ chỉ đường cho người mù công nghệ:\n",
            "🗂 Cần gì thì cứ réo bot qua mấy lệnh dưới đây:\n"
        ]
        
        cmds = (
            f"🛠 HỆ THỐNG & CÔNG CỤ\n"
            f" ├─ {p}ping: Kiểm tra tốc độ mạng\n"
            f" ├─ {p}id: Lấy ID nhóm/cá nhân\n"
            f" ├─ {p}echo: Bot nhại lại câu nói\n"
            f" ├─ {p}search: Tìm info Facebook\n"
            f" └─ {p}unsend: Thu hồi tin (Admin)\n\n"
            f"🎮 GIẢI TRÍ & TRÒ CHUYỆN\n"
            f" ├─ {p}hibot: Trò chuyện ngẫu nhiên\n"
            f" ├─ {p}doanso: Game đoán số bí mật\n"
            f" ├─ {p}noitu: Trò chơi nối từ\n"
            f" └─ {p}huygame: Hủy game đang chơi\n\n"
            f"⏰ TRỢ LÝ NHẮC HẸN\n"
            f" ├─ {p}nhac <Số>m/h <Việc> (Đếm ngược)\n"
            f" │   VD: {p}nhac 15m Tắt bếp\n"
            f" ├─ {p}hen <Giờ> <Việc> (Trong ngày)\n"
            f" │   VD: {p}hen 20:00 Họp nhóm\n"
            f" ├─ {p}lich <Giờ> <Ngày/Năm> <Việc>\n"
            f" │   VD: {p}lich 15:30 25/10/2026 Đám cưới\n"
            f" ├─ {p}ds_nhac: Xem danh sách báo thức\n"
            f" └─ {p}huynhac <Số>: Xóa lịch hẹn"
        )
        self._reply(snap, random.choice(intros) + cmds)

    def _cmd_id(self, snap: dict, arg: str) -> None:
        import random
        id_info = (
            f"🆔 type      : {snap.get('type')}\n"
            f"   threadID  : {snap.get('replyToID')}\n"
            f"   userID    : {snap.get('userID')}\n"
            f"   messageID : {snap.get('messageID')}"
        )
        # Random câu dẫn
        loi_dan = random.choice([
            "Trình lên sếp thông tin định danh đây ạ:\n",
            "Quét radar thành công! Info của nhóm/người này:\n",
            "Đã moi ra được ID gốc, mời sếp check:\n",
            "Hồ sơ tuyệt mật đây, đừng để lộ nha:\n"
        ])
        self._reply(snap, loi_dan + id_info)

    def _cmd_echo(self, snap: dict, arg: str) -> None:
        import random
        if not arg:
            loi_nhac = random.choice([
                f"Cách dùng: {self.prefix}echo <nội dung>. Phải có chữ mới nhại được chứ!",
                "Gõ thiếu rồi má ơi. Thêm nội dung đằng sau lệnh đi.",
                "Định bắt tui nhại lại không khí à? Điền thêm chữ vào!"
            ])
            self._reply(snap, loi_nhac)
            return
            
        kieu_nhai = random.choice([
            arg,
            f"Loa loa loa: {arg}",
            f"Đã nhận thông điệp: {arg}",
            f"Bản sao y chính bản: {arg}"
        ])
        self._reply(snap, kieu_nhai)

    def _cmd_search(self, snap: dict, arg: str) -> None:
        if not arg:
            self._reply(snap, f"Cách dùng: {self.prefix}search <từ khoá>")
            return
        try:
            res = _search.func(self.dataFB, arg)
        except Exception as exc:  # noqa: BLE001
            loi_ky_thuat = [
                f"❌ Lỗi rồi má ơi: {exc}",
                f"Bị Facebook chặn họng rồi, thử lại sau nha (Lỗi: {exc})",
                f"Máy móc dạo này chán quá, tìm không ra (Lỗi: {exc})"
            ]
            self._reply(snap, random.choice(loi_ky_thuat))
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
        import random
        sender_id = str(snap.get("userID") or "")
        
        # Kịch bản 1: Không phải admin mà dám dùng lệnh
        if self.admins and sender_id not in self.admins:
            tu_choi = [
                "⛔ Xin lỗi, bạn chưa đủ trình! Chỉ Admin mới được xài.",
                "Ủa ai cho xài lệnh này? Kêu Admin ra đây nói chuyện!",
                "Quyền lực của bạn bằng 0 ở lệnh này nhé. Đòi làm Admin à?",
                "Bạn tuổi gì đòi thu hồi tin nhắn của tui? 🐧"
            ]
            self._reply(snap, random.choice(tu_choi))
            return

        thread_id = str(snap["replyToID"])
        target = self._last_bot_message.get(thread_id)
        
        # Kịch bản 2: Không có tin nhắn để xóa
        if not target:
            khong_co_tin = [
                "ℹ️ Có tin nào đâu mà thu hồi? Bị lú à?",
                "Ủa tôi có nhắn gì đâu mà bắt thu hồi?",
                "Quét mỏi mắt không thấy tin nhắn nào của tui để xóa.",
                "Mới ngủ dậy, trí nhớ trống rỗng, không biết thu hồi cái nào hết á!"
            ]
            self._reply(snap, random.choice(khong_co_tin))
            return

        result = unsend_message(target, self.dataFB)
        log("unsend", f"{target} -> {result}")
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
        
        # Kịch bản báo lỗi nếu đang có game
        if thread_id in self._games:
            canh_bao = [
                "⚠️ Đang chơi dở ván đoán số rồi, tập trung đoán đi mấy bạn.",
                "Game cũ chưa xong đã đòi mở game mới? Gõ /huygame đi nhé.",
                "Sòng bạc đang mở rồi! Đoán nốt ván hiện tại đi."
            ]
            self._reply(snap, random.choice(canh_bao))
            return
        
        # Bot random số từ 1 đến 100
        so_bi_mat = random.randint(1, 100)
        self._games[thread_id] = so_bi_mat
        
        # Kịch bản mời chào người chơi
        loi_chao = [
            "🎮 TRÒ CHƠI ĐOÁN SỐ BẮT ĐẦU!\nBot đã giấu một con số từ 1 đến 100. Ai có năng lực ngoại cảm thì nhào vô!",
            "🎲 SÒNG BẠC MỞ CỬA!\nTôi đang giữ 1 con số bí mật từ 1-100. Ai đoán trúng được tôn làm thánh!",
            "🔢 THỬ THÁCH NHÂN PHẨM!\nĐố cả nhóm biết tôi đang nghĩ số mấy từ 1 đến 100? Nhắn thẳng số vào đây nha."
        ]
        self._reply(snap, random.choice(loi_chao))

    def _cmd_huygame(self, snap: dict, arg: str) -> None:
        import random
        thread_id = str(snap["replyToID"])
        
        if thread_id in self._games:
            dap_an = self._games.pop(thread_id)
            huy_game = [
                f"🛑 Dẹp dẹp! Nghỉ chơi. Đáp án đúng là: {dap_an}",
                f"🏳️ Bỏ cuộc à? Tưởng thế nào! Đáp án dễ ẹc: {dap_an}",
                f"Thôi giải tán, đoán mãi không ra tốn thời gian. Số bí mật là {dap_an} nha.",
                f"Lần sau nhân phẩm tốt hơn hãy chơi nha. Số trúng thưởng là {dap_an}."
            ]
            self._reply(snap, random.choice(huy_game))
        else:
            khong_co_game = [
                "Hiện tại không có ván game nào để hủy hết á.",
                "Có chơi đâu mà đòi hủy? Ngáo à?",
                "Chưa start game đã đòi end là sao ta?"
            ]
            self._reply(snap, random.choice(khong_co_game))

    def _cmd_noitu(self, snap: dict, arg: str) -> None:
        import random
        thread_id = str(snap["replyToID"])
        
        if thread_id in self._games:
            self._reply(snap, random.choice([
                "⚠️ Đang có game chơi dở kìa mấy má. Tập trung đi!",
                "Chưa xong ván này đòi ván khác? /huygame đi rồi tính.",
                "Sân chơi đang có người dùng rồi nha, từ từ đã."
            ]))
            return
        
        kho_tu = ["con mèo", "hoa hồng", "bầu trời", "máy tính", "gia đình", "tình yêu", "bạn bè", "tương lai"]
        tu_khoi_dau = random.choice(kho_tu)
        chu_cuoi = tu_khoi_dau.split()[1]
        
        self._games[thread_id] = {
            "type": "noitu",
            "last_char": chu_cuoi,
            "used": [tu_khoi_dau],
            "last_user_id": None,  
            "is_checking": False   
        }
        
        mo_dau = random.choice([
            f"🔤 ĐẠI CHIẾN NỐI TỪ KHỞI TRANH!\nLuật: Mỗi người 1 lượt, đợi check xong mới nối.\n\nTừ mồi: 【 {tu_khoi_dau.upper()} 】\n👉 Tiếp theo bắt đầu bằng chữ: '{chu_cuoi.upper()}'",
            f"🔥 LÊN SÀN! Ai vua tiếng Việt thì nhào vô!\nKhông nối 2 lần liên tiếp, cấm xài lại từ cũ nha.\n\nBot đi trước: 【 {tu_khoi_dau.upper()} 】\n👉 Đố ai nối được chữ '{chu_cuoi.upper()}'!"
        ])
        self._reply(snap, mo_dau)

    def _handle_noitu(self, snap: dict, thread_id: str, text: str) -> None:
        import requests
        import random
        
        game = self._games[thread_id]
        user_id = str(snap.get("userID") or "")
        
        if game.get("is_checking") == True:
            return 
            
        # Nối 2 lần liên tiếp
        if user_id == game.get("last_user_id"):
            self._reply(snap, random.choice([
                "🚫 Bớt tham lam! Nhường người khác nối đi chứ.",
                "Ê ê, 1 người không được đi 2 bước liên tục nha!",
                "Định solo một mình hay gì? Đợi người khác nối đã."
            ]))
            return

        words = text.split()
        
        # BỘ LỌC TỪ BẬY
        danh_sach_den = ["cặc", "lồn", "đụ", "đĩ", "buồi", "dái", "cứt", "phò", "nứng", "địt"]
        for tu_bay in danh_sach_den:
            if tu_bay in text:
                self._reply(snap, random.choice([
                    f"🤬 Vô văn hóa! Chữ '{text.upper()}' mà cũng lôi ra được. Nhập lại đi!",
                    f"Thẻ đỏ! Bot hiền chứ không có mù nha. Cấm dùng từ bậy!",
                    f"Cảnh cáo! Nhóm văn minh không xài chữ '{text.upper()}'. Đổi từ khác mau."
                ]))
                return

        # Sai chữ bắt đầu
        if words[0] != game["last_char"]:
            self._reply(snap, random.choice([
                f"❌ Lạc đề! Người ta bảo nối chữ '{game['last_char'].upper()}' mà?",
                f"Ủa đọc lộn đề hả? Bắt đầu bằng chữ '{game['last_char'].upper()}' giùm.",
                f"Mắt để đi đâu đấy? Nối bằng chữ '{game['last_char'].upper()}' cơ mà!"
            ]))
            return
            
        # Bị trùng từ
        if text in game["used"]:
            self._reply(snap, random.choice([
                f"♻️ Tối cổ à? Chữ '{text.upper()}' có người xài rồi!",
                f"Lặp từ kìa! '{text.upper()}' xài rồi, rớt đài, kiếm từ khác đi.",
                f"Bí từ rồi đúng không? '{text.upper()}' bị lấy mất rồi nha."
            ]))
            return
            
        game["is_checking"] = True
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        
        # Check Wikipedia
        try:
            url = f"https://vi.wiktionary.org/w/api.php?action=query&titles={text}&format=json"
            res = requests.get(url, headers=headers, timeout=5).json()
            pages = res.get("query", {}).get("pages", {})
            
            is_valid = True
            for page_id, info in pages.items():
                if int(page_id) < 0 or "missing" in info:
                    is_valid = False
                    
            if not is_valid:
                self._reply(snap, random.choice([
                    f"❓ Chữ '{text.upper()}' tự bịa hả? Từ điển tiếng Việt không có nha!",
                    f"Lại chế từ rồi! Wikipedia không công nhận chữ '{text.upper()}' đâu.",
                    f"'{text.upper()}' nghĩa là gì? Đừng có ghép bừa chứ, từ điển khóc đấy."
                ]))
                game["is_checking"] = False 
                return
        except Exception as e:
            print(f"[Lỗi Từ Điển] {e}")
            self._reply(snap, random.choice([
                "⚠️ Trọng tài đang lag mạng, không tra từ điển được. Gõ lại chữ khác xem sao!",
                "Căng quá đứt mạng rồi, anh em nương tay nhập lại từ khác giúp bot nha."
            ]))
            game["is_checking"] = False 
            return
            
        # Ghi nhận từ hợp lệ
        game["used"].append(text)
        game["last_char"] = words[1]
        game["last_user_id"] = user_id 
        
        user_name = "Người chơi"
        try:
            res = _get_user_info.func(self.dataFB, str(snap.get("userID") or ""))
            if isinstance(res, dict) and "err" not in res:
                user_name = res.get("nameUser") or res.get("firstName") or "Bạn"
        except:
            pass

        # KIỂM TRA ĐƯỜNG CÙNG (CƠ CHẾ KẾT THÚC GAME)
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
                msg_win = random.choice([
                    f"🏆 ÔI THẦN LINH ƠI!\n{user_name} tung quả chốt 【 {text.upper()} 】 đi vào lòng đất, không ai nối tiếp chữ '{game['last_char'].upper()}' được nữa!\n🎉 THẮNG RỒI!",
                    f"👑 ĐỈNH CẤP NHÂN SINH!\nTừ điển cũng bó tay với chữ 【 {text.upper()} 】 của {user_name}!\n🎉 CHÚC MỪNG NHÀ VÔ ĐỊCH!",
                    f"💥 K.O! KNOCK OUT!\n{user_name} vừa chặn mọi đường sống của chữ '{game['last_char'].upper()}'!\n🎉 QUÁ XUẤT SẮC!"
                ])
                self._reply(snap, msg_win)
                return
                
        except Exception as e:
            print(f"[Lỗi Kiểm Tra End Game] {e}")

        # Tiếp tục game với phản hồi khen ngợi ngẫu nhiên
        khen_ngoi = random.choice([
            f"✅ Mượt! {user_name} nối đúng.\n👉 Tiếp: '{game['last_char'].upper()}'...",
            f"Duyệt! {user_name} hay đấy.\n👉 Ai nối được chữ '{game['last_char'].upper()}' nào?",
            f"Hợp lệ nha {user_name}!\n👉 Tới công chuyện với chữ '{game['last_char'].upper()}' đi.",
            f"✅ Quá chuẩn!\n👉 Không chần chờ, bắt đầu bằng '{game['last_char'].upper()}' đi 500 anh em."
        ])
        self._reply(snap, khen_ngoi)
        game["is_checking"] = False

    def _handle_guess(self, snap: dict, thread_id: str, doan: int) -> None:
        import random
        so_bi_mat = self._games[thread_id]
        
        if doan == so_bi_mat:
            # Đoán trúng
            user_id = str(snap.get("userID") or "")
            user_name = "bạn"
            try:
                res = _get_user_info.func(self.dataFB, user_id)
                if isinstance(res, dict) and "err" not in res:
                    user_name = res.get("nameUser") or res.get("firstName") or "bạn"
            except:
                pass
                
            self._games.pop(thread_id)
            
            khen_thuong = [
                f"🎉 BINGO! {user_name.upper()} ĐÃ ĐOÁN TRÚNG SỐ {so_bi_mat}! Đỉnh của chóp!",
                f"🏆 CHẤN ĐỘNG! {user_name.upper()} đọc được suy nghĩ của tôi à? Đáp án chính xác là {so_bi_mat}.",
                f"Thánh đoán đây rồi! Xin chúc mừng {user_name.upper()} lụm giải với con số {so_bi_mat}!"
            ]
            self._reply(snap, random.choice(khen_thuong))
            
        elif doan < so_bi_mat:
            # Đoán nhỏ hơn
            lon_hon = [
                f"📈 Số {doan} bé tí teo, đoán LỚN HƠN coi!",
                f"Yếu quá, số phải bự hơn {doan} cơ.",
                f"Chưa tới nơi rồi, đẩy số lên cao hơn {doan} đi bạn ơi.",
                f"Số {doan} nhỏ quá, mạnh dạn cộng thêm vào đi!"
            ]
            self._reply(snap, random.choice(lon_hon))
            
        else:
            # Đoán lớn hơn
            nho_hon = [
                f"📉 Số {doan} to quá, lố rồi, NHỎ HƠN đi!",
                f"Mạnh tay quá, giảm số xuống dưới {doan} xíu nào.",
                f"Nhỏ lại nhỏ lại, {doan} là bự chà bá lửa luôn á.",
                f"Tụt xuống xíu đi, {doan} lớn quá rồi."
            ]
            self._reply(snap, random.choice(nho_hon))

    def _cmd_nhac(self, snap: dict, arg: str) -> None:
        """Lệnh đếm ngược: /nhac 15m [-t 5m] Tắt bếp"""
        import requests, random
        from datetime import datetime, timedelta
        
        if not arg:
            self._reply(snap, f"⚠️ Cú pháp: {self.prefix}nhac <Số>m/h [-t nhắc_trước] <Việc>\nVD: {self.prefix}nhac 30m -t 5m Đi đón con")
            return
            
        parts = arg.split()
        pre_remind = 0
        
        # Thuật toán bóc tách cờ -t siêu thông minh
        if "-t" in parts:
            idx = parts.index("-t")
            if idx + 1 < len(parts):
                t_val = parts[idx+1]
                try:
                    if t_val.endswith('m'): pre_remind = int(t_val[:-1]) * 60
                    elif t_val.endswith('h'): pre_remind = int(t_val[:-1]) * 3600
                    elif t_val.endswith('s'): pre_remind = int(t_val[:-1])
                except ValueError:
                    pass
                del parts[idx:idx+2]
                
        if len(parts) < 2:
            self._reply(snap, "❌ Thiếu nội dung hoặc thời gian rồi bạn ơi!")
            return
            
        time_str = parts[0].lower()
        message = " ".join(parts[1:])
        
        if len(message) > 200:
            self._reply(snap, random.choice([
                "❌ Dài quá! Ghi chú tối đa 200 ký tự thôi nha.",
                "⚠️ Bot không nhận viết sớ, rút gọn nội dung dưới 200 chữ giùm!"
            ]))
            return

        seconds = 0
        try:
            if time_str.endswith('s'): seconds = int(time_str[:-1])
            elif time_str.endswith('m'): seconds = int(time_str[:-1]) * 60
            elif time_str.endswith('h'): seconds = int(time_str[:-1]) * 3600
            elif time_str.isdigit(): seconds = int(time_str)
            else: raise ValueError
        except ValueError:
            self._reply(snap, "❌ Ký hiệu thời gian sai! Dùng s, m, h (VD: 15m, 2h).")
            return

        if seconds <= 0:
            self._reply(snap, "❌ Lùi thời gian à? Gõ số lớn hơn 0 đi.")
            return
        if seconds > 172800:  # Tối đa đếm ngược 48 giờ
            self._reply(snap, f"❌ Đếm ngược xa vậy? Quá 48h thì bạn dùng lệnh {self.prefix}lich cho lẹ nha!")
            return

        now_vn = datetime.utcnow() + timedelta(hours=7)
        target_time = now_vn + timedelta(seconds=seconds)
        
        self._save_reminder(snap, int(target_time.timestamp()), target_time.strftime("%H:%M %d/%m"), message, time_str, pre_remind)

    def _cmd_hen(self, snap: dict, arg: str) -> None:
        """Lệnh hẹn giờ trong ngày: /hen 20:00 [-t 15m] Họp nhóm"""
        import requests, random
        from datetime import datetime, timedelta
        
        if not arg:
            self._reply(snap, f"⚠️ Cú pháp: {self.prefix}hen HH:MM [-t nhắc_trước] <Việc>\nVD: {self.prefix}hen 20:00 -t 15m Đi học")
            return
            
        parts = arg.split()
        pre_remind = 0
        if "-t" in parts:
            idx = parts.index("-t")
            if idx + 1 < len(parts):
                t_val = parts[idx+1]
                try:
                    if t_val.endswith('m'): pre_remind = int(t_val[:-1]) * 60
                    elif t_val.endswith('h'): pre_remind = int(t_val[:-1]) * 3600
                except: pass
                del parts[idx:idx+2]
                
        if len(parts) < 2:
            self._reply(snap, "❌ Thiếu nội dung hoặc giờ rồi. (VD: /hen 20:00 Ăn cơm)")
            return
            
        time_str = parts[0]
        message = " ".join(parts[1:])
        
        if len(message) > 200:
            self._reply(snap, "❌ Lỗi: Nội dung dài hơn 200 ký tự bot đọc không nổi nha.")
            return

        now_vn = datetime.utcnow() + timedelta(hours=7)
        try:
            target_time = datetime.strptime(f"{time_str} {now_vn.strftime('%d/%m/%Y')}", "%H:%M %d/%m/%Y")
            
            if target_time <= now_vn:
                self._reply(snap, f"⏳ Giờ này của hôm nay qua mất rồi! Nếu định hẹn ngày mai, hãy dùng: {self.prefix}lich {time_str} {(now_vn + timedelta(days=1)).strftime('%d/%m')}")
                return
                
            self._save_reminder(snap, int(target_time.timestamp()), target_time.strftime("%H:%M hôm nay"), message, f"vào {time_str}", pre_remind)
        except ValueError:
            self._reply(snap, "❌ Ghi sai định dạng giờ! Phải là HH:MM (VD: 07:05, 20:30, không có giờ ảo như 25:99 đâu nha).")

    def _cmd_lich(self, snap: dict, arg: str) -> None:
        """Lệnh hẹn ngày xa: /lich 20:00 25/10/2026 Sinh nhật"""
        import requests, random
        from datetime import datetime, timedelta
        
        if not arg:
            self._reply(snap, f"⚠️ Cú pháp: {self.prefix}lich HH:MM DD/MM/YYYY [-t nhắc_trước] <Việc>\nVD: {self.prefix}lich 20:00 25/10/2026 -t 2h Đám cưới")
            return
            
        parts = arg.split()
        pre_remind = 0
        if "-t" in parts:
            idx = parts.index("-t")
            if idx + 1 < len(parts):
                t_val = parts[idx+1]
                try:
                    if t_val.endswith('m'): pre_remind = int(t_val[:-1]) * 60
                    elif t_val.endswith('h'): pre_remind = int(t_val[:-1]) * 3600
                except: pass
                del parts[idx:idx+2]
                
        if len(parts) < 3:
            self._reply(snap, "❌ Thiếu dữ kiện! Cần đủ: Giờ, Ngày/Tháng và Nội dung.")
            return
            
        time_str = parts[0]
        date_str = parts[1]
        message = " ".join(parts[2:])
        
        if len(message) > 200:
            self._reply(snap, "❌ Dài dòng quá! Tóm tắt nội dung dưới 200 chữ thôi bạn.")
            return
            
        now_vn = datetime.utcnow() + timedelta(hours=7)
        try:
            date_parts = date_str.split('/')
            if len(date_parts) == 2:
                target_year = now_vn.year
                target_time = datetime.strptime(f"{time_str} {date_str}/{target_year}", "%H:%M %d/%m/%Y")
                if target_time < now_vn:
                    target_time = datetime.strptime(f"{time_str} {date_str}/{target_year + 1}", "%H:%M %d/%m/%Y")
            elif len(date_parts) == 3:
                if len(date_parts[2]) == 2:
                    date_parts[2] = "20" + date_parts[2]
                    date_str = "/".join(date_parts)
                target_time = datetime.strptime(f"{time_str} {date_str}", "%H:%M %d/%m/%Y")
                if target_time < now_vn:
                    self._reply(snap, "⏳ Cỗ máy thời gian bị hỏng à? Ngày này trong quá khứ mất rồi.")
                    return
            else:
                raise ValueError
                
            self._save_reminder(snap, int(target_time.timestamp()), target_time.strftime("%H:%M %d/%m/%Y"), message, f"ngày {target_time.strftime('%d/%m')} lúc {time_str}", pre_remind)
        except ValueError:
            self._reply(snap, "❌ Lỗi định dạng ngày giờ! Dùng: HH:MM DD/MM/YYYY (VD: 20:00 25/10/2026). Đừng tạo giờ ngày ảo.")

    def _save_reminder(self, snap: dict, target_timestamp: int, display_time: str, message: str, time_label: str, pre_remind: int = 0) -> None:
        """Hàm lưu Firebase hỗ trợ tạo thêm báo thức nhắc sớm"""
        import requests, random
        from datetime import datetime, timedelta
        
        firebase_url = load_config().get("firebase_url")
        thread_id = str(snap["replyToID"])
        now_ts = int((datetime.utcnow() + timedelta(hours=7)).timestamp())
        
        data_to_save = {
            "message": message,
            "user_id": str(snap.get("userID") or ""),
            "type_chat": snap.get("type"),
            "display_time": display_time,
            "is_pre": False
        }
        
        payload = {str(target_timestamp): data_to_save}
        
        # TẠO BÁO THỨC NHẮC TRƯỚC (NẾU CÓ)
        if pre_remind > 0:
            pre_time = target_timestamp - pre_remind
            if pre_time > now_ts:
                pre_data = data_to_save.copy()
                pre_data["message"] = f"[SẮP TỚI HẠN] {message}"
                pre_data["is_pre"] = True
                payload[str(pre_time)] = pre_data
        
        try:
            requests.patch(f"{firebase_url}/reminders/{thread_id}.json", json=payload)
            msg_ok = random.choice([
                f"✅ Đã chốt đơn! Tới {time_label} tui sẽ réo.",
                f"👌 Lên lịch thành công! Cứ an tâm nha.",
                f"Đã lưu vào bộ nhớ đám mây! Tới {time_label} là có mặt.",
                f"Xong! Đồng hồ đã điểm, chờ thông báo nhé.",
                f"Ghi nhận! Bạn cứ làm việc khác đi, việc nhắc để bot lo.",
                f"Set kèo thành công! Tới {time_label} sẽ hú.",
                f"Đã lên dây cót đồng hồ! Chờ tui réo nha.",
                f"Lịch hẹn đã được chốt sổ! An tâm nha."
            ])
            if pre_remind > 0:
                msg_ok += f"\n(🔔 Bot sẽ nhắc sớm trước {pre_remind//60 if pre_remind%3600 else pre_remind//3600}{'m' if pre_remind%3600 else 'h'})"
            self._reply(snap, msg_ok)
        except Exception as e:
            self._reply(snap, f"❌ Lỗi CSDL đám mây: {e}")

    def _cmd_ds_nhac(self, snap: dict, arg: str) -> None:
        import requests, random
        from datetime import datetime
        
        thread_id = str(snap["replyToID"])
        firebase_url = load_config().get("firebase_url")
        
        khong_co_data = [
            "📭 Nhóm mình hiện không có cái hẹn nào hết á.",
            "Sạch bách! Không có báo thức nào đang chờ.",
            "Check rồi, trống trơn nha. Đặt báo thức đi!",
            "Chẳng có lịch hẹn nào cả, dạo này rảnh rỗi ghê.",
            "Hệ thống không ghi nhận báo thức nào của nhóm này.",
            "Lịch trống trơn, ngủ khỏe đi các bạn."
        ]
        
        try:
            res = requests.get(f"{firebase_url}/reminders/{thread_id}.json")
            if res.status_code != 200 or not res.json():
                self._reply(snap, random.choice(khong_co_data))
                return
            
            reminders = res.json()
            sorted_ts = sorted(reminders.keys(), key=lambda x: int(x))
            
            msg = random.choice([
                "📋 DANH SÁCH BÁO THỨC CỦA NHÓM:\n",
                "⏰ CÁC LỊCH HẸN ĐANG CHỜ:\n",
                "🗓 Sổ tay nhắc việc của nhóm mình nè:\n",
                "🔔 Điểm danh các kèo đã set:\n",
                "🗂 Báo cáo danh sách hẹn giờ:\n"
            ])
            
            for idx, ts in enumerate(sorted_ts, 1):
                data = reminders[ts]
                dt = datetime.fromtimestamp(int(ts)).strftime('%H:%M %d/%m')
                icon = "⏳" if data.get("is_pre") else "⏰"
                msg += f"{idx}. {icon} {dt} - {data.get('message')[:40]}\n"
            
            msg += f"\n💡 Để xóa lịch, gõ: {self.prefix}huynhac <Số thứ tự>"
            self._reply(snap, msg)
            
        except Exception as e:
            self._reply(snap, f"❌ Lỗi truy xuất: {e}")

    def _cmd_huynhac(self, snap: dict, arg: str) -> None:
        import requests, random
        thread_id = str(snap["replyToID"])
        firebase_url = load_config().get("firebase_url")
        
        sai_cu_phap = [
            f"⚠️ Phải nhập số thứ tự để xóa. VD: {self.prefix}huynhac 1\n(Xem số bằng lệnh {self.prefix}ds_nhac)",
            f"Xóa cái nào? Đưa số thứ tự đây. VD: {self.prefix}huynhac 2",
            "Chưa nhập ID báo thức kìa bạn ơi.",
            "Ủa xóa báo thức số mấy? Gõ thêm số vào nhé.",
            "Vui lòng kèm theo số thứ tự của báo thức cần hủy."
        ]
        
        if not arg or not arg.isdigit():
            self._reply(snap, random.choice(sai_cu_phap))
            return
            
        idx_to_delete = int(arg)
        
        try:
            res = requests.get(f"{firebase_url}/reminders/{thread_id}.json")
            if res.status_code != 200 or not res.json():
                self._reply(snap, random.choice([
                    "Nhóm có báo thức nào đâu mà đòi xóa?",
                    "Làm gì có lịch hẹn nào mà hủy hả trời?",
                    "Trống trơn rồi, không cần xóa nữa đâu.",
                    "Danh sách sạch bóng rồi, khỏi xóa.",
                    "Vườn không nhà trống, lấy đâu ra báo thức mà xóa."
                ]))
                return
                
            reminders = res.json()
            sorted_ts = sorted(reminders.keys(), key=lambda x: int(x))
            
            if idx_to_delete < 1 or idx_to_delete > len(sorted_ts):
                self._reply(snap, random.choice([
                    "❌ Số thứ tự không tồn tại! Coi lại danh sách đi.",
                    "Tào lao! Làm gì có số này trong danh sách.",
                    "Nhập sai số rồi, kiểm tra lại bằng lệnh ds_nhac nha.",
                    "Vượt quá số lượng báo thức hiện có. Nhập số nhỏ hơn đi.",
                    "Số ảo à? Nhập đúng số thứ tự giùm."
                ]))
                return
                
            ts_to_delete = sorted_ts[idx_to_delete - 1]
            requests.delete(f"{firebase_url}/reminders/{thread_id}/{ts_to_delete}.json")
            
            xoa_ok = [
                "🗑 Đã phi tang báo thức thành công!",
                "✅ Đã hủy kèo này nha.",
                "Xóa lịch hẹn rồi đó, khỏi lo bị réo.",
                "Mission Passed! Báo thức đã bay màu.",
                "Đã gạch tên báo thức này khỏi sổ tay.",
                "Hủy thành công. Cần gì cứ đặt lại nhé."
            ]
            self._reply(snap, random.choice(xoa_ok))
            
        except Exception as e:
            self._reply(snap, f"❌ Lỗi CSDL: {e}")


    # =========================================================================
    # VÒNG LẶP CHẠY NGẦM: QUÉT FIREBASE MỖI PHÚT ĐỂ TÌM BÁO THỨC ĐẾN HẠN
    def _check_reminders_loop(self) -> None:
        import time
        import requests
        import random
        from datetime import datetime, timedelta
        
        firebase_url = load_config().get("firebase_url")
        if not firebase_url: return
            
        while True:
            try:
                # Lấy giờ VN hiện tại
                now_timestamp = int((datetime.utcnow() + timedelta(hours=7)).timestamp())
                
                # Tải toàn bộ lịch hẹn về
                res = requests.get(f"{firebase_url}/reminders.json")
                if res.status_code == 200 and res.json():
                    all_reminders = res.json()
                    
                    for thread_id, timestamps in all_reminders.items():
                        if not timestamps: continue
                        
                        for ts_str, data in list(timestamps.items()):
                            if int(ts_str) <= now_timestamp:
                                # ĐÃ ĐẾN GIỜ -> GỬI THÔNG BÁO
                                user_id = data.get("user_id", "")
                                message = data.get("message", "")
                                
                                user_name = "Bạn"
                                try:
                                    info = _get_user_info.func(self.dataFB, user_id)
                                    if isinstance(info, dict) and "err" not in info:
                                        user_name = info.get("nameUser") or info.get("firstName") or "Bạn"
                                except: pass
                                
                                loi_nhac = random.choice([
                                    f"⏰ TING TING! Tới giờ rồi cả nhà ơi!\n🔔 {user_name} nhắc:\n👉 {message}",
                                    f"🚨 BÁO ĐỘNG TOÀN THỂ NHÓM CHAT!\n🔔 Lịch hẹn của {user_name}:\n👉 {message}",
                                    f"📢 LOA LOA LOA! Có hẹn có hẹn!\n🔔 {user_name} nhờ bot hú:\n👉 {message}",
                                    f"🎯 CHÚ Ý CHÚ Ý!\n🔔 Đến giờ thực thi nhiệm vụ của {user_name}:\n👉 {message}",
                                    f"🚀 ĐẾN GIỜ G RỒI!\n🔔 Lệnh từ {user_name}:\n👉 {message}",
                                    f"🔥 ALO ALO! Bùng cháy lên nào!\n🔔 {user_name} nhắc nhở:\n👉 {message}",
                                    f"💌 CÓ THƯ TỪ TƯƠNG LAI!\n🔔 {user_name} gửi lời nhắn:\n👉 {message}"
                                ])
                                
                                fake_snap = {
                                    "replyToID": thread_id,
                                    "type": data.get("type_chat", "group"),
                                    "userID": user_id
                                }
                                self._reply(fake_snap, loi_nhac)
                                
                                # XÓA BÁO THỨC KHỎI FIREBASE ĐỂ KHÔNG BỊ LẶP LẠI
                                requests.delete(f"{firebase_url}/reminders/{thread_id}/{ts_str}.json")
                                
            except Exception as e:
                pass # Lỗi mạng tạm thời thì bỏ qua, phút sau check lại
                
            time.sleep(60) # Ngủ 60 giây rồi quét tiếp



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
