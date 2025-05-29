"""
텔레그램 봇 - StockBot 원격 제어 및 모니터링
별도 스레드에서 실행되어 실시간 명령 처리
"""
import asyncio
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, TYPE_CHECKING
from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from utils.logger import setup_logger
from utils.korean_time import now_kst, now_kst_time

# 설정 import (settings.py에서 .env 파일을 읽어서 제공)
from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

if TYPE_CHECKING:
    from main import StockBot

logger = setup_logger(__name__)

class TelegramBot:
    """텔레그램 봇 클래스"""

    def __init__(self, stock_bot_instance: Optional['StockBot'] = None):
        # StockBot 메인 인스턴스 참조
        self.stock_bot = stock_bot_instance
        self.main_bot_ref = None  # 메인 봇 참조 추가

        # 텔레그램 설정 (settings.py에서 가져옴)
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID

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

    def set_main_bot_reference(self, main_bot_ref):
        """메인 봇 참조 설정 (데이터베이스 접근용)"""
        self.main_bot_ref = main_bot_ref
        logger.info("🔗 메인 봇 참조 설정 완료")

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
            CommandHandler("refresh", self._cmd_refresh_prices),
            CommandHandler("stats", self._cmd_stats),
            CommandHandler("history", self._cmd_history),
            CommandHandler("todaydb", self._cmd_today_db),
            CommandHandler("export", self._cmd_export),
            CommandHandler("slots", self._cmd_time_slots),
            CommandHandler("selected", self._cmd_selected_stocks),
            CommandHandler("slotperf", self._cmd_slot_performance),
            CommandHandler("existing", self._cmd_existing_positions),
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
            BotCommand("refresh", "REST API 가격 갱신"),
            BotCommand("stats", "거래 통계"),
            BotCommand("history", "거래 내역 DB"),
            BotCommand("todaydb", "오늘 거래 DB"),
            BotCommand("export", "CSV 내보내기"),
            BotCommand("stop", "시스템 종료"),
            BotCommand("slots", "시간대별 종목 선정"),
            BotCommand("selected", "선택된 종목"),
            BotCommand("slotperf", "시간대별 성과"),
            BotCommand("existing", "기존 보유 종목"),
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
            f"⏰ 시작 시간: {now_kst().strftime('%Y-%m-%d %H:%M:%S')}"
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
            "📋 <b>거래 데이터베이스</b>\n"
            "/stats [일수] - 거래 성과 통계 (기본: 7일)\n"
            "/history [일수] - 거래 내역 조회 (기본: 3일)\n"
            "/todaydb - 오늘 거래 요약 (DB)\n"
            "/export [일수] - CSV 내보내기 (기본: 30일)\n\n"
            "🕐 <b>시간대별 종목 선정</b>\n"
            "/slots - 시간대별 종목 선정 현황\n"
            "/selected [날짜] - 선정된 종목 조회\n"
            "/slotperf [일수] - 시간대별 성과 분석\n"
            "/existing - 기존 보유 종목 현황\n\n"
            "🎮 <b>제어 명령</b>\n"
            "/pause - 거래 일시정지\n"
            "/resume - 거래 재개\n"
            "/refresh - REST API 가격 강제 갱신\n"
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

            # 웹소켓 구독 정보 포맷팅
            websocket_status = "❌"
            if status.get('websocket_connected', False):
                websocket_status = f"✅ ({status.get('websocket_subscriptions', 0)}종목)"
            
            # 구독 종목 목록 (최대 5개만 표시)
            subscribed_stocks = status.get('subscribed_stocks', [])
            subscription_info = ""
            if subscribed_stocks:
                displayed_stocks = subscribed_stocks[:5]
                subscription_info = f"\n📋 구독 종목: {', '.join(displayed_stocks)}"
                if len(subscribed_stocks) > 5:
                    subscription_info += f" 외 {len(subscribed_stocks)-5}개"

            message = (
                "📊 <b>StockBot 시스템 상태</b>\n\n"
                f"🔄 봇 실행: {'✅' if status.get('bot_running', False) else '❌'}\n"
                f"⏸️ 일시정지: {'✅' if self.bot_paused else '❌'}\n"
                f"📡 WebSocket: {websocket_status}\n"
                f"🔗 웹소켓 사용량: {status.get('websocket_usage', '0/41')}{subscription_info}\n"
                f"💼 보유 포지션: {status.get('positions_count', 0)}개\n"
                f"📋 대기 주문: {status.get('pending_orders_count', 0)}개\n"
                f"📝 거래 내역: {status.get('order_history_count', 0)}건\n\n"
                f"🕐 스케줄러: {status.get('scheduler', {}).get('current_slot', 'None')}\n"
                f"📈 활성 전략: {len(status.get('scheduler', {}).get('active_strategies', []))}\n"
                f"🎯 활성 종목: {status.get('scheduler', {}).get('total_active_stocks', 0)}개\n\n"
                f"⏰ 확인 시간: {now_kst().strftime('%H:%M:%S')}"
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
        """시스템 종료 처리 - signal 기반 안전 종료"""
        try:
            import time
            import signal
            time.sleep(2)  # 메시지 전송 대기

            logger.info("🛑 텔레그램 /stop 명령: 시스템 종료 시작")

            if self.stock_bot:
                # StockBot의 signal handler 호출 (안전한 종료)
                logger.info("📢 StockBot에 종료 신호 전송")
                self.stock_bot._signal_handler(signal.SIGTERM, None)
            else:
                # stock_bot이 없으면 직접 프로그램 종료
                logger.warning("⚠️ stock_bot 인스턴스가 없음 - 직접 종료")
                import sys
                sys.exit(0)

        except Exception as e:
            logger.error(f"시스템 종료 중 오류: {e}")
            # 마지막 수단으로 강제 종료
            import sys
            sys.exit(1)

    async def _cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """계좌 잔고 조회 - KIS API 연동"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return

        try:
            if not self.stock_bot:
                await update.message.reply_text("❌ StockBot 인스턴스에 접근할 수 없습니다.")
                return

            # 로딩 메시지 전송
            loading_msg = await update.message.reply_text("⏳ 잔고 조회 중...")

            # 🔧 StockBot의 get_balance() 메서드 사용 (TradingManager에 위임)
            try:
                balance_data = self.stock_bot.trading_manager.get_balance()

                if not balance_data or not balance_data.get('success'):
                    error_msg = balance_data.get('message', '잔고 조회 실패') if balance_data else '응답 없음'
                    raise Exception(error_msg)

                # TradingManager.get_balance()의 실제 반환 구조 사용
                total_assets = balance_data.get('total_assets', 0)         # 총평가금액
                available_cash = balance_data.get('available_cash', 0)     # 가용현금
                stock_evaluation = balance_data.get('stock_evaluation', 0) # 주식평가금액
                profit_loss = balance_data.get('profit_loss', 0)           # 평가손익
                holdings = balance_data.get('holdings', [])                # 보유종목

                # 수익률 계산 (투자원금 기준)
                investment_amount = total_assets - profit_loss if total_assets > profit_loss else total_assets
                profit_rate = (profit_loss / investment_amount * 100) if investment_amount > 0 else 0

                # 포맷팅
                total_assets_str = f"{total_assets:,}" if total_assets else "0"
                available_cash_str = f"{available_cash:,}" if available_cash else "0"
                stock_eval_str = f"{stock_evaluation:,}" if stock_evaluation else "0"
                profit_str = f"{profit_loss:+,}" if profit_loss else "0"

                # 수익률 색상
                profit_emoji = "📈" if profit_loss > 0 else "📉" if profit_loss < 0 else "➖"

                message = (
                    f"💰 <b>계좌 잔고</b>\n\n"
                    f"💵 총 평가금액: {total_assets_str}원\n"
                    f"🏦 가용 현금: {available_cash_str}원\n"
                    f"📊 주식 평가금액: {stock_eval_str}원\n"
                    f"{profit_emoji} 평가손익: {profit_str}원 ({profit_rate:+.2f}%)\n"
                    f"📦 보유 종목: {len(holdings)}개\n\n"
                    f"⏰ {now_kst().strftime('%Y-%m-%d %H:%M:%S')}"
                )

                await loading_msg.edit_text(message, parse_mode='HTML')

            except Exception as api_error:
                logger.error(f"KIS API 잔고 조회 오류: {api_error}")

                # API 오류 시 fallback - 포지션 매니저 데이터 사용
                await loading_msg.edit_text("⚠️ API 오류 - 포지션 데이터로 대체합니다...")

                # 포지션 매니저에서 현재 포지션 정보 가져오기
                try:
                    if hasattr(self.stock_bot, 'position_manager') and self.stock_bot.position_manager:
                        positions = self.stock_bot.position_manager.get_positions('active')
                        total_positions = len(positions)
                        position_value = 0

                        for stock_code, position in positions.items():
                            qty = position.get('quantity', 0)
                            buy_price = position.get('buy_price', 0)
                            position_value += qty * buy_price

                        fallback_message = (
                            f"💰 <b>계좌 정보 (로컬 추정)</b>\n\n"
                            f"📊 보유 종목: {total_positions}개\n"
                            f"💵 포지션 추정가치: {position_value:,}원\n\n"
                            f"⚠️ 정확한 잔고는 증권사 앱에서 확인하세요.\n"
                            f"🔧 API 오류: {str(api_error)[:50]}..."
                        )

                        await loading_msg.edit_text(fallback_message, parse_mode='HTML')
                    else:
                        await loading_msg.edit_text(
                            f"❌ API 오류 및 로컬 데이터 없음\n"
                            f"🔧 오류: {str(api_error)[:50]}...\n"
                            f"증권사 앱에서 직접 확인해주세요."
                        )
                except Exception as fallback_error:
                    logger.error(f"Fallback 데이터 조회 오류: {fallback_error}")
                    await loading_msg.edit_text(
                        f"❌ 모든 데이터 소스 접근 실패\n"
                        f"🔧 주 오류: {str(api_error)[:30]}...\n"
                        f"🔧 보조 오류: {str(fallback_error)[:30]}..."
                    )

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
                f"⏰ {now_kst().strftime('%Y-%m-%d %H:%M:%S')}"
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
                f"📅 {now_kst().strftime('%Y-%m-%d')}\n\n"
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

            message += f"\n⏰ {now_kst().strftime('%H:%M:%S')}"

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

            message += f"\n⏰ {now_kst().strftime('%H:%M:%S')}"

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

            message += f"⏰ {now_kst().strftime('%H:%M:%S')}"

            await update.message.reply_text(message, parse_mode='HTML')

        except Exception as e:
            logger.error(f"활성 종목 조회 오류: {e}")
            await update.message.reply_text("❌ 활성 종목 조회 중 오류가 발생했습니다.")

    async def _cmd_refresh_prices(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """REST API 가격 강제 갱신"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return

        try:
            if not self.stock_bot:
                await update.message.reply_text("❌ StockBot 인스턴스에 접근할 수 없습니다.")
                return

            # 로딩 메시지
            loading_msg = await update.message.reply_text("🔄 <b>REST API로 현재가 강제 갱신 중...</b>", parse_mode='HTML')

            # 포지션 매니저 확인
            if not hasattr(self.stock_bot, 'position_manager') or not self.stock_bot.position_manager:
                await loading_msg.edit_text("❌ 포지션 매니저에 접근할 수 없습니다.")
                return

            # 현재 포지션 확인
            active_positions = self.stock_bot.position_manager.get_positions(status='active')
            
            if not active_positions:
                await loading_msg.edit_text("📋 현재 보유 중인 포지션이 없습니다.")
                return

            # REST API 강제 갱신 실행
            start_time = time.time()
            updated_count = self.stock_bot.position_manager.force_price_update_via_rest_api()
            elapsed = time.time() - start_time

            # 업데이트된 포지션 정보 수집
            updated_positions = self.stock_bot.position_manager.get_positions(status='active')
            
            position_list = []
            for stock_code, position in updated_positions.items():
                current_price = position.get('current_price', 0)
                profit_rate = position.get('profit_rate', 0)
                last_update = position.get('last_update', 0)
                
                # 최근 업데이트 확인 (30초 이내)
                recently_updated = "🆕" if time.time() - last_update < 30 else "⏰"
                
                position_list.append(
                    f"{recently_updated} {stock_code}: {current_price:,}원 "
                    f"({'📈' if profit_rate >= 0 else '📉'}{profit_rate:+.1f}%)"
                )

            # 결과 메시지
            if updated_count > 0:
                message = (
                    f"✅ <b>REST API 가격 갱신 완료</b>\n\n"
                    f"📊 갱신된 종목: {updated_count}/{len(active_positions)}개\n"
                    f"⏱️ 소요 시간: {elapsed:.1f}초\n\n"
                    f"📋 <b>현재 포지션 상태</b>\n"
                )
                
                # 포지션이 많으면 일부만 표시
                if len(position_list) <= 8:
                    message += "\n".join(position_list)
                else:
                    message += "\n".join(position_list[:8])
                    message += f"\n\n📝 총 {len(position_list)}개 포지션 (8개만 표시)"
                
                message += f"\n\n⏰ 갱신 시간: {now_kst().strftime('%H:%M:%S')}"
                
            else:
                message = (
                    f"⚠️ <b>가격 갱신 실패</b>\n\n"
                    f"❌ 갱신된 종목: 0/{len(active_positions)}개\n"
                    f"🔧 웹소켓이나 REST API 연결을 확인해주세요.\n\n"
                    f"⏰ 시도 시간: {now_kst().strftime('%H:%M:%S')}"
                )

            await loading_msg.edit_text(message, parse_mode='HTML')

        except Exception as e:
            logger.error(f"가격 갱신 오류: {e}")
            try:
                await loading_msg.edit_text(f"❌ 가격 갱신 중 오류가 발생했습니다.\n\n🔧 오류: {str(e)[:50]}...")
            except:
                await update.message.reply_text("❌ 가격 갱신 중 오류가 발생했습니다.")

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
            import threading
            
            # 별도 스레드에서 비동기 알림 실행
            def run_async_notification():
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self.send_notification(message))
                    finally:
                        loop.close()
                except Exception as e:
                    logger.error(f"스레드 내 알림 전송 실패: {e}")
            
            # 별도 스레드에서 실행 (메인 이벤트 루프 간섭 방지)
            notification_thread = threading.Thread(target=run_async_notification, daemon=True)
            notification_thread.start()
            
            # 최대 3초 대기 (블로킹 방지)
            notification_thread.join(timeout=3.0)
            
        except Exception as e:
            logger.error(f"동기 알림 전송 실패: {e}")

    # ========== 거래 알림 메서드들 (main.py에서 이동) ==========

    def send_order_notification(self, order_type: str, stock_code: str, quantity: int, price: int, strategy: str):
        """주문 알림 전송"""
        try:
            total_amount = quantity * price
            message = (
                f"🎯 {order_type} 주문 체결\n"
                f"종목: {stock_code}\n"
                f"수량: {quantity:,}주\n"
                f"가격: {price:,}원\n"
                f"금액: {total_amount:,}원\n"
                f"전략: {strategy}\n"
                f"시간: {datetime.now().strftime('%H:%M:%S')}"
            )
            self.send_notification_sync(message)
        except Exception as e:
            logger.error(f"주문 알림 전송 오류: {e}")

    def send_auto_sell_notification(self, sell_signal: dict, order_no: str):
        """자동 매도 알림 전송"""
        try:
            stock_code = sell_signal['stock_code']
            quantity = sell_signal['quantity']
            current_price = sell_signal['current_price']
            auto_sell_price = sell_signal.get('auto_sell_price', current_price)
            profit_rate = sell_signal['profit_rate']
            reason = sell_signal['reason']

            total_amount = quantity * auto_sell_price

            message = (
                f"🤖 자동 매도 주문 완료\n"
                f"종목: {stock_code}\n"
                f"수량: {quantity:,}주\n"
                f"주문가: {auto_sell_price:,}원\n"
                f"현재가: {current_price:,}원\n"
                f"주문금액: {total_amount:,}원\n"
                f"수익률: {profit_rate:.2f}%\n"
                f"사유: {reason}\n"
                f"주문번호: {order_no}\n"
                f"시간: {datetime.now().strftime('%H:%M:%S')}"
            )
            self.send_notification_sync(message)
        except Exception as e:
            logger.error(f"자동 매도 알림 전송 오류: {e}")

    def send_signal_notification(self, signal: dict):
        """신호 알림 전송"""
        try:
            message = (
                f"📊 거래신호 감지\n"
                f"종목: {signal['stock_code']}\n"
                f"신호: {signal['signal_type']}\n"
                f"전략: {signal['strategy']}\n"
                f"가격: {signal.get('price', 'N/A')}\n"
                f"시간: {datetime.now().strftime('%H:%M:%S')}"
            )
            self.send_notification_sync(message)
        except Exception as e:
            logger.error(f"신호 알림 전송 오류: {e}")

    def send_general_notification(self, message: str):
        """일반 알림 전송"""
        try:
            self.send_notification_sync(message)
        except Exception as e:
            logger.error(f"일반 알림 전송 오류: {e}")

    def send_hourly_report(self, report: str):
        """1시간 리포트 전송"""
        try:
            message = f"📊 1시간 리포트\n{report}"
            self.send_notification_sync(message)
        except Exception as e:
            logger.error(f"리포트 전송 오류: {e}")

    def send_startup_notification(self):
        """StockBot 시작 알림 전송"""
        if not self.stock_bot:
            logger.warning("📱 StockBot 참조가 없어 시작 알림을 보낼 수 없습니다")
            return
            
        try:
            market_status = self.stock_bot._check_market_status()
            
            message = (
                f"🤖 <b>StockBot 시작 완료!</b>\n\n"
                f"📅 시간: {market_status['current_date']} {market_status['current_time']} (KST)\n"
                f"📈 시장 상태: {market_status['status']}\n"
                f"🔗 웹소켓: {'연결됨' if self.stock_bot.websocket_manager and hasattr(self.stock_bot.websocket_manager, 'is_connected') and self.stock_bot.websocket_manager.is_connected else '준비중'}\n"
                f"📅 전략 스케줄러: 활성화\n\n"
                f"💬 /help 명령어로 사용법을 확인하세요."
            )
            
            logger.info("📨 텔레그램 시작 알림 전송 시도")
            self.send_notification_sync(message)
            logger.info("✅ 텔레그램 시작 알림 전송 완료")
            
        except Exception as e:
            logger.error(f"❌ 텔레그램 시작 알림 전송 실패: {e}")

    # ========== 데이터베이스 관련 명령어들 ==========

    async def _cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """거래 통계 조회"""
        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            # 기간 파라미터 처리
            days = 7  # 기본 7일
            if context.args:
                try:
                    days = int(context.args[0])
                    days = max(1, min(days, 365))  # 1~365일 제한
                except:
                    pass

            await update.message.reply_text(f"📊 최근 {days}일 거래 통계 조회 중...")
            
            # 거래 성과 통계 조회
            stats = self.main_bot_ref.trade_db.get_performance_stats(days=days)
            
            if not stats:
                await update.message.reply_text("📊 거래 통계 데이터가 없습니다")
                return

            # 통계 메시지 구성
            message = f"📊 **거래 성과 통계** (최근 {days}일)\n\n"
            
            # 기본 통계
            message += f"🔢 **거래 현황**\n"
            message += f"• 총 거래: {stats['total_trades']}건 (매수: {stats['buy_trades']}, 매도: {stats['sell_trades']})\n"
            message += f"• 완료된 거래: {stats['completed_trades']}건\n"
            message += f"• 승률: {stats['win_rate']}% ({stats['winning_trades']}승 {stats['losing_trades']}패)\n\n"
            
            # 수익 통계
            message += f"💰 **수익 현황**\n"
            message += f"• 총 손익: {stats['total_profit_loss']:+,}원\n"
            message += f"• 평균 수익률: {stats['avg_profit_rate']:+.2f}%\n"
            message += f"• 최대 수익: {stats['max_profit']:+,}원\n"
            message += f"• 최대 손실: {stats['max_loss']:+,}원\n"
            message += f"• 평균 보유시간: {stats['avg_holding_minutes']:.1f}분\n\n"
            
            # 전략별 성과
            if stats['strategy_performance']:
                message += f"📈 **전략별 성과**\n"
                for strategy in stats['strategy_performance']:
                    message += f"• {strategy['strategy']}: {strategy['total_profit']:+,}원 ({strategy['avg_profit_rate']:+.1f}%, {strategy['trade_count']}건)\n"

            await update.message.reply_text(message, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"거래 통계 조회 오류: {e}")
            await update.message.reply_text(f"❌ 통계 조회 실패: {str(e)}")

    async def _cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """거래 내역 조회"""
        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            # 파라미터 처리
            days = 3  # 기본 3일
            limit = 10  # 기본 10건
            
            if context.args:
                try:
                    days = int(context.args[0])
                    days = max(1, min(days, 30))  # 1~30일 제한
                except:
                    pass

            await update.message.reply_text(f"📋 최근 {days}일 거래 내역 조회 중...")
            
            # 거래 내역 조회
            trades = self.main_bot_ref.trade_db.get_trade_history(days=days)
            
            if not trades:
                await update.message.reply_text("📋 거래 내역이 없습니다")
                return

            # 최근 거래 내역 표시 (최대 limit건)
            recent_trades = trades[:limit]
            
            message = f"📋 **최근 거래 내역** (최근 {days}일, {len(recent_trades)}/{len(trades)}건)\n\n"
            
            for trade in recent_trades:
                timestamp = trade['timestamp'][:16]  # YYYY-MM-DD HH:MM
                trade_type = "🟢 매수" if trade['trade_type'] == 'BUY' else "🔴 매도"
                
                message += f"{trade_type} `{trade['stock_code']}`\n"
                message += f"• 시간: {timestamp}\n"
                message += f"• 수량: {trade['quantity']:,}주 @ {trade['price']:,}원\n"
                message += f"• 금액: {trade['total_amount']:,}원\n"
                
                # 매도의 경우 수익 정보 추가
                if trade['trade_type'] == 'SELL' and trade['profit_loss'] is not None:
                    profit_emoji = "📈" if trade['profit_loss'] > 0 else "📉"
                    message += f"• 손익: {profit_emoji} {trade['profit_loss']:+,}원 ({trade['profit_rate']:+.1f}%)\n"
                    message += f"• 보유: {trade['holding_duration']}분\n"
                
                message += f"• 전략: {trade['strategy_type']}\n\n"

            # 메시지가 너무 길면 분할
            if len(message) > 4000:
                # 첫 5건만 표시
                message = f"📋 **최근 거래 내역** (최근 {days}일, 5/{len(trades)}건)\n\n"
                for trade in recent_trades[:5]:
                    timestamp = trade['timestamp'][:16]
                    trade_type = "🟢 매수" if trade['trade_type'] == 'BUY' else "🔴 매도"
                    
                    message += f"{trade_type} `{trade['stock_code']}`\n"
                    message += f"• {timestamp}, {trade['quantity']:,}주 @ {trade['price']:,}원\n"
                    if trade['trade_type'] == 'SELL' and trade['profit_loss'] is not None:
                        profit_emoji = "📈" if trade['profit_loss'] > 0 else "📉"
                        message += f"• 손익: {profit_emoji} {trade['profit_loss']:+,}원 ({trade['profit_rate']:+.1f}%)\n"
                    message += "\n"

            await update.message.reply_text(message, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"거래 내역 조회 오류: {e}")
            await update.message.reply_text(f"❌ 내역 조회 실패: {str(e)}")

    async def _cmd_today_db(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오늘 거래 요약 (데이터베이스)"""
        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            await update.message.reply_text("📅 오늘 거래 요약 조회 중...")
            
            # 오늘 거래 통계
            today_stats = self.main_bot_ref.trade_db.get_performance_stats(days=1)
            daily_summary = self.main_bot_ref.trade_db.get_daily_summary(days=1)
            
            if not today_stats:
                await update.message.reply_text("📅 오늘 거래 데이터가 없습니다")
                return

            message = f"📅 **오늘 거래 요약**\n\n"
            
            # 기본 정보
            message += f"🔢 **거래 현황**\n"
            message += f"• 총 거래: {today_stats['total_trades']}건\n"
            message += f"• 매수: {today_stats['buy_trades']}건, 매도: {today_stats['sell_trades']}건\n"
            message += f"• 완료된 거래: {today_stats['completed_trades']}건\n\n"
            
            # 수익 현황
            if today_stats['completed_trades'] > 0:
                message += f"💰 **수익 현황**\n"
                message += f"• 총 손익: {today_stats['total_profit_loss']:+,}원\n"
                message += f"• 평균 수익률: {today_stats['avg_profit_rate']:+.2f}%\n"
                message += f"• 승률: {today_stats['win_rate']}% ({today_stats['winning_trades']}승 {today_stats['losing_trades']}패)\n"
                
                if today_stats['max_profit'] > 0:
                    message += f"• 최고 수익: +{today_stats['max_profit']:,}원\n"
                if today_stats['max_loss'] < 0:
                    message += f"• 최대 손실: {today_stats['max_loss']:,}원\n"
                
                message += f"• 평균 보유시간: {today_stats['avg_holding_minutes']:.1f}분\n\n"
            
            # 전략별 성과
            if today_stats['strategy_performance']:
                message += f"📈 **전략별 성과**\n"
                for strategy in today_stats['strategy_performance']:
                    message += f"• {strategy['strategy']}: {strategy['total_profit']:+,}원 ({strategy['trade_count']}건)\n"

            await update.message.reply_text(message, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"오늘 거래 요약 오류: {e}")
            await update.message.reply_text(f"❌ 요약 조회 실패: {str(e)}")

    async def _cmd_export(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """거래 내역 CSV 내보내기"""
        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            # 기간 파라미터 처리
            days = 30  # 기본 30일
            if context.args:
                try:
                    days = int(context.args[0])
                    days = max(1, min(days, 365))
                except:
                    pass

            await update.message.reply_text(f"📄 최근 {days}일 거래 내역 CSV 생성 중...")
            
            # CSV 파일 생성
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"data/trades_export_{timestamp}.csv"
            
            success = self.main_bot_ref.trade_db.export_trades_to_csv(csv_filename, days=days)
            
            if success:
                # 파일 전송
                try:
                    with open(csv_filename, 'rb') as file:
                        await update.message.reply_document(
                            document=file,
                            filename=f"거래내역_{timestamp}.csv",
                            caption=f"📄 최근 {days}일 거래 내역 CSV 파일"
                        )
                except Exception as e:
                    logger.error(f"CSV 파일 전송 오류: {e}")
                    await update.message.reply_text(f"✅ CSV 생성 완료: {csv_filename}\n❌ 파일 전송 실패: {str(e)}")
            else:
                await update.message.reply_text("❌ CSV 생성 실패")
                
        except Exception as e:
            logger.error(f"CSV 내보내기 오류: {e}")
            await update.message.reply_text(f"❌ CSV 생성 실패: {str(e)}")

    async def _cmd_time_slots(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시간대별 종목 선정 현황"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return

        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            # 오늘 선정된 종목들 조회
            selected_stocks = self.main_bot_ref.trade_db.get_selected_stocks_by_date()

            if not selected_stocks:
                await update.message.reply_text("📊 오늘 선정된 종목이 없습니다.")
                return

            # 시간대별로 그룹화
            time_slots = {}
            for stock in selected_stocks:
                slot = stock['time_slot']
                if slot not in time_slots:
                    time_slots[slot] = []
                time_slots[slot].append(stock)

            message = "🕐 <b>오늘의 시간대별 종목 선정</b>\n\n"
            
            for slot_name, stocks in time_slots.items():
                message += f"📍 <b>{slot_name}</b> ({stocks[0]['slot_start_time']} ~ {stocks[0]['slot_end_time']})\n"
                
                # 전략별로 그룹화
                strategies = {}
                for stock in stocks:
                    strategy = stock['strategy_type']
                    if strategy not in strategies:
                        strategies[strategy] = []
                    strategies[strategy].append(stock)
                
                for strategy_name, strategy_stocks in strategies.items():
                    message += f"  📈 <b>{strategy_name}</b>: {len(strategy_stocks)}개\n"
                    
                    # 상위 3개만 표시
                    for i, stock in enumerate(strategy_stocks[:3]):
                        status = "✅" if stock['is_activated'] else "⏸️"
                        trade_status = "💰" if stock['trade_executed'] else ""
                        message += f"    {i+1}. {stock['stock_code']} {status}{trade_status} ({stock['score']:.1f}점)\n"
                    
                    if len(strategy_stocks) > 3:
                        message += f"    ... 외 {len(strategy_stocks)-3}개\n"
                
                message += "\n"

            await update.message.reply_text(message, parse_mode='HTML')

        except Exception as e:
            logger.error(f"시간대별 종목 선정 현황 오류: {e}")
            await update.message.reply_text(f"❌ 조회 중 오류가 발생했습니다: {str(e)}")

    async def _cmd_selected_stocks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """선정된 종목 상세 조회"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return

        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            # 날짜 파라미터 처리 (기본: 오늘)
            target_date = None
            if context.args:
                try:
                    from datetime import datetime
                    target_date = datetime.strptime(context.args[0], '%Y-%m-%d').date()
                except ValueError:
                    await update.message.reply_text("❌ 날짜 형식이 잘못되었습니다. (예: 2024-01-01)")
                    return

            # 선정된 종목들 조회
            selected_stocks = self.main_bot_ref.trade_db.get_selected_stocks_by_date(target_date)

            if not selected_stocks:
                date_str = target_date.strftime('%Y-%m-%d') if target_date else '오늘'
                await update.message.reply_text(f"📊 {date_str} 선정된 종목이 없습니다.")
                return

            message = f"📊 <b>선정된 종목 상세</b>\n"
            if target_date:
                message += f"📅 {target_date.strftime('%Y-%m-%d')}\n\n"
            else:
                message += f"📅 오늘\n\n"

            # 점수 기준으로 정렬
            sorted_stocks = sorted(selected_stocks, key=lambda x: x['score'], reverse=True)

            for i, stock in enumerate(sorted_stocks[:10]):  # 상위 10개만
                status_icons = []
                if stock['is_activated']:
                    status_icons.append("✅활성")
                if stock['activation_success']:
                    status_icons.append("📡실시간")
                if stock['trade_executed']:
                    status_icons.append("💰거래")
                
                status = " ".join(status_icons) if status_icons else "⏸️대기"
                
                message += f"{i+1}. <b>{stock['stock_code']}</b> ({stock['strategy_type']})\n"
                message += f"   점수: {stock['score']:.1f} | 순위: {stock['rank_in_strategy']}\n"
                message += f"   현재가: {stock['current_price']:,}원 ({stock['change_rate']:+.1f}%)\n"
                message += f"   상태: {status}\n"
                message += f"   이유: {stock['reason']}\n\n"

            if len(selected_stocks) > 10:
                message += f"... 외 {len(selected_stocks)-10}개 종목"

            await update.message.reply_text(message, parse_mode='HTML')

        except Exception as e:
            logger.error(f"선정된 종목 조회 오류: {e}")
            await update.message.reply_text(f"❌ 조회 중 오류가 발생했습니다: {str(e)}")

    async def _cmd_slot_performance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시간대별 성과 분석"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return

        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            # 기간 파라미터 처리
            days = 7  # 기본 7일
            if context.args:
                try:
                    days = int(context.args[0])
                    days = max(1, min(days, 30))  # 1~30일 제한
                except ValueError:
                    await update.message.reply_text("❌ 일수는 숫자로 입력해주세요.")
                    return

            # 시간대별 성과 조회
            performance_data = self.main_bot_ref.trade_db.get_time_slot_performance(days)

            if not performance_data:
                await update.message.reply_text(f"📊 최근 {days}일간 시간대별 성과 데이터가 없습니다.")
                return

            message = f"📊 <b>시간대별 성과 분석</b> (최근 {days}일)\n\n"

            # 시간대별로 그룹화
            time_slots = {}
            for data in performance_data:
                slot = data['time_slot']
                if slot not in time_slots:
                    time_slots[slot] = []
                time_slots[slot].append(data)

            for slot_name, strategies in time_slots.items():
                # 시간대 전체 통계 계산
                total_candidates = sum(s['total_candidates'] for s in strategies)
                total_activated = sum(s['activated_count'] for s in strategies)
                total_traded = sum(s['traded_count'] for s in strategies)
                total_profit = sum(s['total_profit'] for s in strategies)
                avg_score = sum(s['avg_score'] * s['total_candidates'] for s in strategies) / total_candidates if total_candidates > 0 else 0

                message += f"🕐 <b>{slot_name}</b>\n"
                message += f"📊 후보: {total_candidates}개 | 활성: {total_activated}개 | 거래: {total_traded}개\n"
                message += f"💰 총손익: {total_profit:+,}원 | 평균점수: {avg_score:.1f}\n"
                
                # 전략별 세부 성과
                best_strategy = max(strategies, key=lambda x: x['total_profit'])
                if best_strategy['total_profit'] != 0:
                    message += f"🏆 최고 전략: {best_strategy['strategy_type']} ({best_strategy['total_profit']:+,}원)\n"
                
                message += "\n"

            await update.message.reply_text(message, parse_mode='HTML')

        except Exception as e:
            logger.error(f"시간대별 성과 분석 오류: {e}")
            await update.message.reply_text(f"❌ 분석 중 오류가 발생했습니다: {str(e)}")

    async def _cmd_existing_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """기존 보유 종목 조회"""
        if not self._check_authorization(update.effective_user.id):
            await update.message.reply_text("❌ 권한이 없습니다.")
            return

        try:
            if not hasattr(self, 'main_bot_ref') or not self.main_bot_ref:
                await update.message.reply_text("❌ 봇 참조를 찾을 수 없습니다")
                return
            
            if not hasattr(self.main_bot_ref, 'trade_db'):
                await update.message.reply_text("❌ 거래 데이터베이스를 찾을 수 없습니다")
                return

            # 기존 보유 종목 조회
            existing_positions = self.main_bot_ref.trade_db.get_existing_positions()

            if not existing_positions:
                await update.message.reply_text("📊 기존 보유 종목이 없습니다.")
                return

            message = "📊 <b>기존 보유 종목</b>\n\n"

            for pos in existing_positions[:5]:  # 최대 5개만 표시
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

            if len(existing_positions) > 5:
                message += f"... 외 {len(existing_positions) - 5}개 더"

            await update.message.reply_text(message, parse_mode='HTML')

        except Exception as e:
            logger.error(f"기존 보유 종목 조회 오류: {e}")
            await update.message.reply_text("❌ 기존 보유 종목 조회 중 오류가 발생했습니다.")
