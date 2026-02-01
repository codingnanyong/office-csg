"""
Flet-based web dashboard for API monitoring
"""

import asyncio
import flet as ft
import logging
from datetime import datetime
from typing import Dict, List, Optional

from ..core.config import settings
from ..models.service import MonitoredService, ServiceStatus, HealthStatus, ServiceOverview
from .monitor import get_service_monitor
from .discovery import get_service_discovery

logger = logging.getLogger(__name__)


class ApiDashboard:
    """API 모니터링 대시보드"""
    
    def __init__(self):
        self.monitor = get_service_monitor()
        self.discovery = get_service_discovery()
        self.page: Optional[ft.Page] = None
        self.refresh_timer = None
        
        # UI 컴포넌트들
        self.overview_cards = {}
        self.service_cards = {}
        self.services_column = ft.Column()
        self.overview_row = ft.Row()
    
    async def create_dashboard(self, page: ft.Page):
        """대시보드 페이지 생성"""
        self.page = page
        
        # 페이지 설정
        page.title = settings.dashboard_title
        page.theme_mode = ft.ThemeMode.LIGHT
        page.padding = 20
        page.scroll = ft.ScrollMode.AUTO
        
        # 헤더 생성
        header = self._create_header()
        
        # 개요 카드 생성
        overview_section = self._create_overview_section()
        
        # 서비스 목록 생성
        services_section = self._create_services_section()
        
        # 페이지에 컴포넌트 추가
        page.add(
            header,
            ft.Divider(height=20, color=ft.colors.TRANSPARENT),
            overview_section,
            ft.Divider(height=30, color=ft.colors.TRANSPARENT),
            services_section
        )
        
        # 초기 데이터 로드
        await self._refresh_data()
        
        # 자동 새로고침 시작
        self._start_auto_refresh()
        
        await page.update_async()
    
    def _create_header(self) -> ft.Container:
        """헤더 섹션 생성"""
        refresh_button = ft.ElevatedButton(
            text="새로고침",
            icon=ft.icons.REFRESH,
            on_click=self._on_refresh_click,
            style=ft.ButtonStyle(
                color={ft.MaterialState.DEFAULT: ft.colors.WHITE},
                bgcolor={ft.MaterialState.DEFAULT: ft.colors.BLUE_600}
            )
        )
        
        last_updated = ft.Text(
            "마지막 업데이트: -",
            size=12,
            color=ft.colors.GREY_600
        )
        
        return ft.Container(
            content=ft.Row([
                ft.Column([
                    ft.Text(
                        settings.dashboard_title,
                        size=28,
                        weight=ft.FontWeight.BOLD,
                        color=ft.colors.BLUE_800
                    ),
                    last_updated
                ], expand=True),
                refresh_button
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            padding=ft.padding.symmetric(vertical=10)
        )
    
    def _create_overview_section(self) -> ft.Container:
        """개요 섹션 생성"""
        self.overview_row = ft.Row([
            self._create_overview_card("전체 서비스", "0", ft.colors.BLUE_600, ft.icons.APPS),
            self._create_overview_card("온라인", "0", ft.colors.GREEN_600, ft.icons.CHECK_CIRCLE),
            self._create_overview_card("오프라인", "0", ft.colors.RED_600, ft.icons.ERROR),
            self._create_overview_card("경고", "0", ft.colors.ORANGE_600, ft.icons.WARNING)
        ], wrap=True, spacing=20, run_spacing=20)
        
        return ft.Container(
            content=ft.Column([
                ft.Text(
                    "서비스 개요",
                    size=20,
                    weight=ft.FontWeight.BOLD,
                    color=ft.colors.GREY_800
                ),
                ft.Divider(height=10, color=ft.colors.TRANSPARENT),
                self.overview_row
            ]),
            padding=ft.padding.all(20),
            border_radius=10,
            bgcolor=ft.colors.GREY_50
        )
    
    def _create_overview_card(self, title: str, value: str, color: str, icon: str) -> ft.Container:
        """개요 카드 생성"""
        card_content = ft.Column([
            ft.Row([
                ft.Icon(icon, color=color, size=30),
                ft.Text(value, size=24, weight=ft.FontWeight.BOLD, color=color)
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Text(title, size=14, color=ft.colors.GREY_600)
        ], spacing=10)
        
        card = ft.Container(
            content=card_content,
            padding=ft.padding.all(20),
            border_radius=10,
            bgcolor=ft.colors.WHITE,
            border=ft.border.all(1, ft.colors.GREY_300),
            width=200,
            height=100
        )
        
        self.overview_cards[title] = card
        return card
    
    def _create_services_section(self) -> ft.Container:
        """서비스 목록 섹션 생성"""
        self.services_column = ft.Column([], spacing=15)
        
        return ft.Container(
            content=ft.Column([
                ft.Text(
                    "서비스 상태",
                    size=20,
                    weight=ft.FontWeight.BOLD,
                    color=ft.colors.GREY_800
                ),
                ft.Divider(height=10, color=ft.colors.TRANSPARENT),
                self.services_column
            ]),
            padding=ft.padding.all(20),
            border_radius=10,
            bgcolor=ft.colors.GREY_50
        )
    
    def _create_service_card(self, service: MonitoredService) -> ft.Container:
        """서비스 카드 생성"""
        # 상태에 따른 색상 설정
        status_colors = {
            ServiceStatus.ONLINE: (ft.colors.GREEN_600, ft.colors.GREEN_50),
            ServiceStatus.OFFLINE: (ft.colors.RED_600, ft.colors.RED_50),
            ServiceStatus.DEGRADED: (ft.colors.ORANGE_600, ft.colors.ORANGE_50),
            ServiceStatus.UNKNOWN: (ft.colors.GREY_600, ft.colors.GREY_50)
        }
        
        status_icons = {
            ServiceStatus.ONLINE: ft.icons.CHECK_CIRCLE,
            ServiceStatus.OFFLINE: ft.icons.ERROR,
            ServiceStatus.DEGRADED: ft.icons.WARNING,
            ServiceStatus.UNKNOWN: ft.icons.HELP
        }
        
        color, bg_color = status_colors.get(service.status, status_colors[ServiceStatus.UNKNOWN])
        icon = status_icons.get(service.status, ft.icons.HELP)
        
        # 서비스 기본 정보
        service_info = ft.Column([
            ft.Row([
                ft.Icon(icon, color=color, size=24),
                ft.Text(
                    service.display_name or service.name,
                    size=18,
                    weight=ft.FontWeight.BOLD,
                    color=ft.colors.GREY_800
                ),
                ft.Container(
                    content=ft.Text(
                        service.status.value.upper(),
                        size=12,
                        weight=ft.FontWeight.BOLD,
                        color=ft.colors.WHITE
                    ),
                    padding=ft.padding.symmetric(horizontal=8, vertical=4),
                    border_radius=12,
                    bgcolor=color
                )
            ], alignment=ft.MainAxisAlignment.START, spacing=10),
            
            ft.Text(
                service.description or f"{service.name} microservice",
                size=14,
                color=ft.colors.GREY_600
            )
        ], spacing=8)
        
        # 엔드포인트 정보
        endpoints_info = self._create_endpoints_info(service.endpoints)
        
        # 메트릭 정보
        metrics_info = self._create_metrics_info(service)
        
        # Kubernetes 정보
        k8s_info = self._create_kubernetes_info(service)
        
        card_content = ft.Column([
            service_info,
            ft.Divider(height=5, color=ft.colors.TRANSPARENT),
            endpoints_info,
            metrics_info,
            k8s_info
        ], spacing=10)
        
        card = ft.Container(
            content=card_content,
            padding=ft.padding.all(20),
            border_radius=10,
            bgcolor=ft.colors.WHITE,
            border=ft.border.all(2, color),
            shadow=ft.BoxShadow(
                spread_radius=1,
                blur_radius=5,
                color=ft.colors.with_opacity(0.1, ft.colors.BLACK)
            )
        )
        
        return card
    
    def _create_endpoints_info(self, endpoints: List) -> ft.Container:
        """엔드포인트 정보 표시"""
        if not endpoints:
            return ft.Container()
        
        endpoint_chips = []
        for endpoint in endpoints:
            # 엔드포인트 상태에 따른 색상
            if endpoint.last_checked and endpoint.status_code:
                if 200 <= endpoint.status_code < 400:
                    chip_color = ft.colors.GREEN_600
                else:
                    chip_color = ft.colors.RED_600
            else:
                chip_color = ft.colors.GREY_600
            
            response_time_text = f" ({endpoint.response_time:.0f}ms)" if endpoint.response_time else ""
            
            chip = ft.Container(
                content=ft.Text(
                    f"{endpoint.name}{response_time_text}",
                    size=12,
                    color=ft.colors.WHITE
                ),
                padding=ft.padding.symmetric(horizontal=8, vertical=4),
                border_radius=8,
                bgcolor=chip_color
            )
            endpoint_chips.append(chip)
        
        return ft.Container(
            content=ft.Column([
                ft.Text("엔드포인트", size=12, weight=ft.FontWeight.BOLD, color=ft.colors.GREY_700),
                ft.Row(endpoint_chips, wrap=True, spacing=5, run_spacing=5)
            ], spacing=5)
        )
    
    def _create_metrics_info(self, service: MonitoredService) -> ft.Container:
        """메트릭 정보 표시"""
        if not service.metrics:
            return ft.Container()
        
        metrics_items = []
        
        if service.metrics.avg_response_time:
            metrics_items.append(
                ft.Text(f"평균 응답시간: {service.metrics.avg_response_time:.0f}ms", size=12)
            )
        
        if service.last_health_check:
            time_diff = datetime.now() - service.last_health_check
            minutes_ago = int(time_diff.total_seconds() / 60)
            metrics_items.append(
                ft.Text(f"마지막 체크: {minutes_ago}분 전", size=12)
            )
        
        if not metrics_items:
            return ft.Container()
        
        return ft.Container(
            content=ft.Column([
                ft.Text("메트릭", size=12, weight=ft.FontWeight.BOLD, color=ft.colors.GREY_700),
                ft.Column(metrics_items, spacing=2)
            ], spacing=5)
        )
    
    def _create_kubernetes_info(self, service: MonitoredService) -> ft.Container:
        """Kubernetes 정보 표시"""
        if not service.kubernetes_info:
            return ft.Container()
        
        k8s = service.kubernetes_info
        
        info_items = [
            ft.Text(f"Replicas: {k8s.ready_replicas}/{k8s.replicas}", size=12),
            ft.Text(f"Namespace: {k8s.namespace}", size=12),
        ]
        
        if k8s.cluster_ip:
            info_items.append(ft.Text(f"Cluster IP: {k8s.cluster_ip}", size=12))
        
        return ft.Container(
            content=ft.Column([
                ft.Text("Kubernetes", size=12, weight=ft.FontWeight.BOLD, color=ft.colors.GREY_700),
                ft.Column(info_items, spacing=2)
            ], spacing=5)
        )
    
    async def _refresh_data(self):
        """데이터 새로고침"""
        try:
            logger.info("Refreshing dashboard data")
            
            # 모든 서비스 체크
            services = await self.monitor.check_all_services()
            
            # 개요 정보 업데이트
            await self._update_overview(services)
            
            # 서비스 카드 업데이트
            await self._update_service_cards(services)
            
            # 마지막 업데이트 시간 표시
            if self.page:
                # 헤더의 마지막 업데이트 시간 찾아서 업데이트
                await self.page.update_async()
            
            logger.info(f"Dashboard refreshed: {len(services)} services")
            
        except Exception as e:
            logger.error(f"Error refreshing dashboard data: {e}")
    
    async def _update_overview(self, services: Dict[str, MonitoredService]):
        """개요 카드 업데이트"""
        total = len(services)
        online = sum(1 for s in services.values() if s.status == ServiceStatus.ONLINE)
        offline = sum(1 for s in services.values() if s.status == ServiceStatus.OFFLINE)
        degraded = sum(1 for s in services.values() if s.status == ServiceStatus.DEGRADED)
        
        # 개요 카드 업데이트
        cards_data = [
            ("전체 서비스", str(total)),
            ("온라인", str(online)),
            ("오프라인", str(offline)),
            ("경고", str(degraded))
        ]
        
        for title, value in cards_data:
            if title in self.overview_cards:
                card = self.overview_cards[title]
                # 카드 내용 업데이트
                if hasattr(card.content, 'controls'):
                    row = card.content.controls[0]  # Row
                    if hasattr(row, 'controls') and len(row.controls) > 1:
                        text_control = row.controls[1]  # Text
                        text_control.value = value
    
    async def _update_service_cards(self, services: Dict[str, MonitoredService]):
        """서비스 카드 업데이트"""
        # 기존 카드 제거
        self.services_column.controls.clear()
        self.service_cards.clear()
        
        # 서비스를 상태별로 정렬 (온라인 -> 경고 -> 오프라인 -> 알 수 없음)
        status_order = {
            ServiceStatus.ONLINE: 0,
            ServiceStatus.DEGRADED: 1,
            ServiceStatus.OFFLINE: 2,
            ServiceStatus.UNKNOWN: 3
        }
        
        sorted_services = sorted(
            services.items(),
            key=lambda x: (status_order.get(x[1].status, 999), x[0])
        )
        
        # 새 카드 생성
        for service_name, service in sorted_services:
            card = self._create_service_card(service)
            self.services_column.controls.append(card)
            self.service_cards[service_name] = card
    
    async def _on_refresh_click(self, e):
        """새로고침 버튼 클릭 핸들러"""
        await self._refresh_data()
    
    def _start_auto_refresh(self):
        """자동 새로고침 시작"""
        if self.refresh_timer:
            return
        
        async def auto_refresh():
            while True:
                await asyncio.sleep(settings.dashboard_refresh_interval)
                if self.page:
                    await self._refresh_data()
        
        self.refresh_timer = asyncio.create_task(auto_refresh())
    
    def _stop_auto_refresh(self):
        """자동 새로고침 중지"""
        if self.refresh_timer:
            self.refresh_timer.cancel()
            self.refresh_timer = None


async def main(page: ft.Page):
    """메인 대시보드 함수"""
    dashboard = ApiDashboard()
    await dashboard.create_dashboard(page)


def start_dashboard_server():
    """대시보드 서버 시작"""
    logger.info(f"Starting Flet dashboard server on port {settings.flet_server_port}")
    
    ft.app(
        target=main,
        port=settings.flet_server_port,
        host="0.0.0.0"
    )