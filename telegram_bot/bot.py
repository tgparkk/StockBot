"""
텔레그램 봇 - StockBot 원격 제어 및 모니터링
별도 스레드에서 실행되어 실시간 명령 처리
"""
import asyncio
import os
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from utils.logger import setup_logger
from database.db_manager import db_manager

# 환경변수 로드
load_dotenv()

logger = setup_logger(__name__)

class TelegramBot:
    """텔레그램 봇 클래스"""
    
    def __init__(self, stock_bot_instance=None):
        # StockBot 메인 인스턴스 참조
        self.stock_bot = stock_bot_instance
        
        # 텔레그램 설정
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 설정되지 않았습니다.")
        
        # 봇 애플리케이션
        self.application = None
        self.running = False
        self.bot_thread = None
        
        # 상태 관리
        self.bot_paused = False
        self.authorized_users = set([int(self.chat_id)])  # 승인된 사용자만 접근
        
        logger.info("🤖 텔레그램 봇 초기화 완료")
    
    def start_bot(self):
        """별도 스레드에서 봇 시작"""
        if self.running:
            logger.warning("텔레그램 봇이 이미 실행 중입니다.")
            return
        
        self.running = True
        self.bot_thread = threading.Thread(target=self._run_bot_sync, daemon=True)
        self.bot_thread.start()
        logger.info("🚀 텔레그램 봇 스레드 시작")
    
    def _run_bot_sync(self):
        """동기 방식으로 봇 실행 (스레드에서 호출)"""
        try:
            # 새 이벤트 루프 생성 (스레드용)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # 봇 실행
            loop.run_until_complete(self._run_bot_async())
            
        except Exception as e:
            logger.error(f"텔레그램 봇 실행 오류: {e}")
        finally:
            self.running = False
    
    async def _run_bot_async(self):
        """비동기 방식으로 봇 실행"""
        try:
            # 애플리케이션 생성
            self.application = Application.builder().token(self.bot_token).build()
            
            # 명령어 핸들러 등록
            await self._register_handlers()
            
            # 봇 명령어 설정
            await self._set_bot_commands()
            
            # 시작 메시지 전송
            await self._send_startup_message()
            
            # 봇 시작
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            logger.info("✅ 텔레그램 봇 시작 완료")
            
            # 봇이 중지될 때까지 대기
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"텔레그램 봇 비동기 실행 오류: {e}")
        finally:
            if self.application:
                await self.application.stop()
                await self.application.shutdown()
    
    async def _register_handlers(self):
        """명령어 핸들러 등록"""
        handlers = [
            CommandHandler("start", self._cmd_start),
            CommandHandler("help", self._cmd_help),
            CommandHandler("status", self._cmd_status),
            CommandHandler("pause", self._cmd_pause),
            CommandHandler("resume", self._cmd_resume),
            CommandHandler("stop", self._cmd_stop),
            CommandHandler("balance", self._cmd_balance),
            CommandHandler("profit", self._cmd_profit),
            CommandHandler("positions", self._cmd_positions),
            CommandHandler("trades", self._cmd_trades),
            CommandHandler("today", self._cmd_today_summary),
            CommandHandler("scheduler", self._cmd_scheduler_status),
            CommandHandler("stocks", self._cmd_active_stocks),
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        ]
        
        for handler in handlers:
            self.application.add_handler(handler)
    
    async def _set_bot_commands(self):
        """봇 명령어 메뉴 설정"""
        commands = [
            BotCommand("start", "봇 시작"),
            BotCommand("help", "도움말"),
            BotCommand("status", "시스템 상태"),
            BotCommand("pause", "거래 일시정지"),
            BotCommand("resume", "거래 재개"),
            BotCommand("balance", "계좌 잔고"),
            BotCommand("profit", "오늘 수익률"),
            BotCommand("positions", "현재 포지션"),
            BotCommand("trades", "거래 내역"),
            BotCommand("today", "오늘 요약"),
            BotCommand("scheduler", "스케줄러 상태"),
            BotCommand("stocks", "활성 종목"),
            BotCommand("stop", "시스템 종료"),
        ]
        
        await self.application.bot.set_my_commands(commands)
    
    def _check_authorization(self, user_id: int) -> bool:
        """사용자 권한 확인"""
        return user_id in self.authorized_users
    
    async def _send_startup_message(self):
        """시작 메시지 전송"""
        message = (
            "🤖 <b>StockBot 텔레그램 연결 완료!</b>\n\n"
            "📊 실시간 모니터링이 시작되었습니다.\n"
            "💬 /help 명령어로 사용법을 확인하세요.\n\n"
            f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        try:
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"시작 메시지 전송 실패: {e}")
    
    # ========== 명령어 핸들러들 ==========
    
    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시작 명령어"""
        user_id = update.effective_user.id
        
        if not self._check_authorization(user_id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        message = (
            "🚀 <b>StockBot 원격 제어 시스템</b>\n\n"
            "📈 실시간 주식 자동매매 봇이 실행 중입니다.\n"
            "💻 텔레그램을 통해 원격으로 제어할 수 있습니다.\n\n"
            "🔧 /help - 명령어 목록 보기"
        )
        
        await update.message.reply_text(message, parse_mode='HTML')
    
    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """도움말"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        message = (
            "🤖 <b>StockBot 명령어 목록</b>\n\n"
            "📊 <b>상태 조회</b>\n"
            "/status - 시스템 전체 상태\n"
            "/scheduler - 전략 스케줄러 상태\n"
            "/stocks - 현재 활성 종목\n"
            "/today - 오늘 거래 요약\n\n"
            "💰 <b>계좌 정보</b>\n"
            "/balance - 계좌 잔고\n"
            "/profit - 오늘 수익률\n"
            "/positions - 현재 포지션\n"
            "/trades - 최근 거래 내역\n\n"
            "🎮 <b>제어 명령</b>\n"
            "/pause - 거래 일시정지\n"
            "/resume - 거래 재개\n"
            "/stop - 시스템 종료\n\n"
            "❓ /help - 이 도움말"
        )
        
        await update.message.reply_text(message, parse_mode='HTML')
    
    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시스템 상태"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            if not self.stock_bot:
                await update.message.reply_text("❌ StockBot 인스턴스에 접근할 수 없습니다.")
                return
            
            status = self.stock_bot.get_system_status()
            
            message = (
                "📊 <b>StockBot 시스템 상태</b>\n\n"
                f"🔄 봇 실행: {'✅' if status.get('bot_running', False) else '❌'}\n"
                f"⏸️ 일시정지: {'✅' if self.bot_paused else '❌'}\n"
                f"📡 WebSocket: {'✅' if status.get('websocket_connected', False) else '❌'}\n"
                f"💼 보유 포지션: {status.get('positions_count', 0)}개\n"
                f"📋 대기 주문: {status.get('pending_orders_count', 0)}개\n"
                f"📝 거래 내역: {status.get('order_history_count', 0)}건\n\n"
                f"🕐 스케줄러: {status.get('scheduler', {}).get('current_slot', 'None')}\n"
                f"📈 활성 전략: {len(status.get('scheduler', {}).get('active_strategies', []))}\n"
                f"🎯 활성 종목: {status.get('scheduler', {}).get('total_active_stocks', 0)}개\n\n"
                f"⏰ 확인 시간: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"상태 조회 오류: {e}")
            await update.message.reply_text("❌ 상태 조회 중 오류가 발생했습니다.")
    
    async def _cmd_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """거래 일시정지"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        self.bot_paused = True
        message = "⏸️ <b>거래 일시정지됨</b>\n\n새로운 거래가 중단되었습니다.\n/resume 명령어로 재개할 수 있습니다."
        await update.message.reply_text(message, parse_mode='HTML')
        logger.info("📢 텔레그램을 통해 거래 일시정지됨")
    
    async def _cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """거래 재개"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        self.bot_paused = False
        message = "▶️ <b>거래 재개됨</b>\n\n정상적인 거래가 재개되었습니다."
        await update.message.reply_text(message, parse_mode='HTML')
        logger.info("📢 텔레그램을 통해 거래 재개됨")
    
    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시스템 종료"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        message = "🛑 <b>시스템 종료 명령 수신</b>\n\nStockBot을 안전하게 종료합니다..."
        await update.message.reply_text(message, parse_mode='HTML')
        
        # 별도 스레드에서 종료 처리
        threading.Thread(target=self._shutdown_system, daemon=True).start()
        
        logger.info("📢 텔레그램을 통해 시스템 종료 명령 수신")
    
    def _shutdown_system(self):
        """시스템 종료 처리"""
        try:
            import time
            time.sleep(2)  # 메시지 전송 대기
            
            if self.stock_bot:
                # StockBot 종료 (비동기 처리)
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.stock_bot.cleanup())
            
            # 봇 종료
            self.stop_bot()
            
            # 프로그램 전체 종료
            os._exit(0)
            
        except Exception as e:
            logger.error(f"시스템 종료 중 오류: {e}")
    
    async def _cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """계좌 잔고 조회"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            if not self.stock_bot or not self.stock_bot.trading_api:
                await update.message.reply_text("❌ 거래 API에 접근할 수 없습니다.")
                return
            
            # KIS API를 통한 잔고 조회 (임시 - 실제 구현 필요)
            message = (
                "💰 <b>계좌 잔고</b>\n\n"
                "🏦 예수금: 조회 중...\n"
                "📊 주식 평가금액: 조회 중...\n"
                "💵 총 평가금액: 조회 중...\n\n"
                "⚠️ 실제 API 연동이 필요합니다."
            )
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"잔고 조회 오류: {e}")
            await update.message.reply_text("❌ 잔고 조회 중 오류가 발생했습니다.")
    
    async def _cmd_profit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오늘 수익률"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            # 오늘 성과 데이터 조회
            today_performance = db_manager.calculate_daily_performance()
            
            if not today_performance:
                await update.message.reply_text("📊 오늘 거래 데이터가 없습니다.")
                return
            
            realized_pnl = today_performance.get('realized_pnl', 0)
            win_count = today_performance.get('win_count', 0)
            lose_count = today_performance.get('lose_count', 0)
            win_rate = today_performance.get('win_rate', 0)
            total_trades = today_performance.get('total_trades', 0)
            
            profit_emoji = "📈" if realized_pnl > 0 else "📉" if realized_pnl < 0 else "➖"
            
            message = (
                f"{profit_emoji} <b>오늘의 수익률</b>\n\n"
                f"💰 실현 손익: {realized_pnl:+,}원\n"
                f"🎯 총 거래: {total_trades}건\n"
                f"✅ 수익 거래: {win_count}건\n"
                f"❌ 손실 거래: {lose_count}건\n"
                f"📊 승률: {win_rate:.1f}%\n\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"수익률 조회 오류: {e}")
            await update.message.reply_text("❌ 수익률 조회 중 오류가 발생했습니다.")
    
    async def _cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """현재 포지션"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            positions = db_manager.get_open_positions()
            
            if not positions:
                await update.message.reply_text("📊 현재 보유 중인 포지션이 없습니다.")
                return
            
            message = "📊 <b>현재 포지션</b>\n\n"
            
            for pos in positions[:5]:  # 최대 5개만 표시
                stock_code = pos['stock_code']
                quantity = pos['quantity']
                avg_price = pos['avg_buy_price']
                unrealized_pnl = pos.get('unrealized_pnl', 0)
                pnl_rate = pos.get('unrealized_pnl_rate', 0)
                
                pnl_emoji = "📈" if unrealized_pnl > 0 else "📉" if unrealized_pnl < 0 else "➖"
                
                message += (
                    f"{pnl_emoji} <b>{stock_code}</b>\n"
                    f"  📊 {quantity:,}주 @ {avg_price:,}원\n"
                    f"  💰 {unrealized_pnl:+,}원 ({pnl_rate:+.2f}%)\n\n"
                )
            
            if len(positions) > 5:
                message += f"... 외 {len(positions) - 5}개 더"
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"포지션 조회 오류: {e}")
            await update.message.reply_text("❌ 포지션 조회 중 오류가 발생했습니다.")
    
    async def _cmd_trades(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """최근 거래 내역"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            trades = db_manager.get_trade_history(days=1)  # 오늘 거래만
            
            if not trades:
                await update.message.reply_text("📊 오늘 거래 내역이 없습니다.")
                return
            
            message = "📝 <b>오늘 거래 내역</b>\n\n"
            
            for trade in trades[:10]:  # 최대 10개만 표시
                stock_code = trade['stock_code']
                order_type = trade['order_type']
                executed_qty = trade['executed_quantity']
                executed_price = trade['executed_price']
                order_time = trade['order_time']
                
                # 시간 파싱
                if isinstance(order_time, str):
                    time_obj = datetime.fromisoformat(order_time)
                    time_str = time_obj.strftime('%H:%M')
                else:
                    time_str = order_time.strftime('%H:%M')
                
                emoji = "🔴" if order_type == 'buy' else "🔵"
                type_text = "매수" if order_type == 'buy' else "매도"
                
                message += (
                    f"{emoji} {time_str} <b>{stock_code}</b>\n"
                    f"  {type_text} {executed_qty:,}주 @ {executed_price:,}원\n\n"
                )
            
            if len(trades) > 10:
                message += f"... 외 {len(trades) - 10}건 더"
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"거래 내역 조회 오류: {e}")
            await update.message.reply_text("❌ 거래 내역 조회 중 오류가 발생했습니다.")
    
    async def _cmd_today_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오늘 요약"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            # 종합 요약 정보
            today_performance = db_manager.calculate_daily_performance()
            selected_stocks = db_manager.get_today_selected_stocks()
            open_positions = db_manager.get_open_positions()
            
            # 시간대별 선택 종목 수
            time_slot_counts = {}
            for stock in selected_stocks:
                slot = stock['time_slot']
                time_slot_counts[slot] = time_slot_counts.get(slot, 0) + 1
            
            realized_pnl = today_performance.get('realized_pnl', 0) if today_performance else 0
            total_trades = today_performance.get('total_trades', 0) if today_performance else 0
            win_rate = today_performance.get('win_rate', 0) if today_performance else 0
            
            profit_emoji = "📈" if realized_pnl > 0 else "📉" if realized_pnl < 0 else "➖"
            
            message = (
                f"📊 <b>오늘 종합 요약</b>\n"
                f"📅 {datetime.now().strftime('%Y-%m-%d')}\n\n"
                f"{profit_emoji} <b>수익 현황</b>\n"
                f"💰 실현 손익: {realized_pnl:+,}원\n"
                f"🎯 거래 건수: {total_trades}건\n"
                f"📊 승률: {win_rate:.1f}%\n\n"
                f"📈 <b>포지션 현황</b>\n"
                f"💼 보유 종목: {len(open_positions)}개\n"
                f"🎯 선택 종목: {len(selected_stocks)}개\n\n"
                f"🕐 <b>시간대별 선택</b>\n"
            )
            
            for slot, count in time_slot_counts.items():
                slot_name = {
                    'golden_time': '골든타임',
                    'morning_leaders': '주도주',
                    'lunch_time': '점심시간',
                    'closing_trend': '마감추세'
                }.get(slot, slot)
                message += f"  {slot_name}: {count}개\n"
            
            message += f"\n⏰ {datetime.now().strftime('%H:%M:%S')}"
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"오늘 요약 조회 오류: {e}")
            await update.message.reply_text("❌ 오늘 요약 조회 중 오류가 발생했습니다.")
    
    async def _cmd_scheduler_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """스케줄러 상태"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            if not self.stock_bot or not hasattr(self.stock_bot, 'strategy_scheduler'):
                await update.message.reply_text("❌ 스케줄러에 접근할 수 없습니다.")
                return
            
            scheduler_status = self.stock_bot.strategy_scheduler.get_status()
            
            current_slot = scheduler_status.get('current_slot', 'None')
            current_phase = scheduler_status.get('current_phase', 'None')
            active_strategies = scheduler_status.get('active_strategies', [])
            total_stocks = scheduler_status.get('total_active_stocks', 0)
            candidates_count = scheduler_status.get('candidates_count', {})
            
            slot_name = {
                'golden_time': '🌅 골든타임 (09:00-09:30)',
                'morning_leaders': '🚀 주도주 (09:30-11:30)',
                'lunch_time': '🍽️ 점심시간 (11:30-14:00)',
                'closing_trend': '📈 마감추세 (14:00-15:20)'
            }.get(current_slot, f'❓ {current_slot}')
            
            message = (
                "🕐 <b>전략 스케줄러 상태</b>\n\n"
                f"📍 현재 시간대: {slot_name}\n"
                f"⚙️ 현재 단계: {current_phase}\n"
                f"🎯 활성 전략: {len(active_strategies)}개\n"
                f"📊 활성 종목: {total_stocks}개\n\n"
                f"🔍 <b>후보 종목 수</b>\n"
            )
            
            for strategy, count in candidates_count.items():
                strategy_name = {
                    'gap_trading': '갭 트레이딩',
                    'volume_breakout': '거래량 돌파',
                    'momentum': '모멘텀'
                }.get(strategy, strategy)
                message += f"  {strategy_name}: {count}개\n"
            
            message += f"\n⏰ {datetime.now().strftime('%H:%M:%S')}"
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"스케줄러 상태 조회 오류: {e}")
            await update.message.reply_text("❌ 스케줄러 상태 조회 중 오류가 발생했습니다.")
    
    async def _cmd_active_stocks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """현재 활성 종목"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return
        
        try:
            selected_stocks = db_manager.get_today_selected_stocks()
            active_stocks = [s for s in selected_stocks if s.get('is_active', False)]
            
            if not active_stocks:
                await update.message.reply_text("📊 현재 활성화된 종목이 없습니다.")
                return
            
            message = "📈 <b>현재 활성 종목</b>\n\n"
            
            # 시간대별로 그룹화
            time_slots = {}
            for stock in active_stocks:
                slot = stock['time_slot']
                if slot not in time_slots:
                    time_slots[slot] = []
                time_slots[slot].append(stock)
            
            for slot, stocks in time_slots.items():
                slot_name = {
                    'golden_time': '🌅 골든타임',
                    'morning_leaders': '🚀 주도주',
                    'lunch_time': '🍽️ 점심시간',
                    'closing_trend': '📈 마감추세'
                }.get(slot, slot)
                
                message += f"<b>{slot_name}</b>\n"
                
                for stock in stocks[:5]:  # 각 시간대별 최대 5개
                    code = stock['stock_code']
                    strategy = stock['strategy_type']
                    reason = stock['reason']
                    
                    strategy_emoji = {
                        'gap_trading': '🔥',
                        'volume_breakout': '🚀',
                        'momentum': '📈'
                    }.get(strategy, '📊')
                    
                    message += f"  {strategy_emoji} {code} - {reason}\n"
                
                if len(stocks) > 5:
                    message += f"  ... 외 {len(stocks) - 5}개 더\n"
                
                message += "\n"
            
            message += f"⏰ {datetime.now().strftime('%H:%M:%S')}"
            
            await update.message.reply_text(message, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"활성 종목 조회 오류: {e}")
            await update.message.reply_text("❌ 활성 종목 조회 중 오류가 발생했습니다.")
    
    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """일반 메시지 처리"""
        if not self._check_authorization(update.effective_user.id):
            return
        
        text = update.message.text.lower()
        
        # 간단한 키워드 응답
        if "안녕" in text or "hello" in text:
            await update.message.reply_text("👋 안녕하세요! StockBot입니다. /help를 입력해보세요.")
        elif "상태" in text:
            await self._cmd_status(update, context)
        elif "수익" in text or "profit" in text:
            await self._cmd_profit(update, context)
        else:
            await update.message.reply_text("❓ 잘 모르겠습니다. /help로 명령어를 확인해보세요.")
    
    # ========== 봇 제어 ==========
    
    def stop_bot(self):
        """봇 중지"""
        self.running = False
        logger.info("🛑 텔레그램 봇 중지")
    
    def is_running(self) -> bool:
        """봇 실행 상태 확인"""
        return self.running
    
    def is_paused(self) -> bool:
        """일시정지 상태 확인"""
        return self.bot_paused
    
    async def send_notification(self, message: str, parse_mode: str = 'HTML'):
        """알림 메시지 전송"""
        if not self.application or not self.running:
            return
        
        try:
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=parse_mode
            )
        except Exception as e:
            logger.error(f"알림 전송 실패: {e}")
    
    def send_notification_sync(self, message: str):
        """동기 방식 알림 전송 (스레드에서 호출용)"""
        if not self.running:
            return
        
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.send_notification(message))
        except Exception as e:
            logger.error(f"동기 알림 전송 실패: {e}")
