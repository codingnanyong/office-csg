# flet_montrg MSA 확장 제안서

## 📋 현재 상황 분석

### 기존 서비스 구조 및 포트 할당
- ✅ **thresholds-service** (포트 30001): 임계치 CRUD 관리
- ✅ **location-service** (포트 30002): 위치 및 센서 정보 관리
- ✅ **realtime-service** (포트 30003): 실시간 데이터 조회 (thresholds, location 의존)
- ✅ **aggregation-service** (포트 30004): 기간별 집계 데이터 조회
- ✅ **integrated-swagger-service** (포트 30005): 통합 API 문서

### 사용 가능한 포트
- **30006**: alert-service (예정)
- **30007**: alert-subscription-service (예정)
- **30008**: alert-notification-service (예정)
- **30009**: sensor-threshold-mapping-service (예정)
- **30010**: alert-evaluation-service (예정, 내부 서비스 - 외부 노출 불필요)
- **30011+**: 향후 확장용

### 새로운 요구사항 (ERD 기반)
- 📊 **alerts**: 알람 발생 이력 저장
- 📧 **alert_subscriptions**: 알람 구독 관리 (factory/building/floor/area 레벨)
- 📨 **alert_notifications**: 메일 발송 이력
- 🔗 **sensor_threshold_map**: 센서별 임계치 매핑

---

## 🏗️ MSA 확장 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                    Alert Domain Services                     │
└─────────────────────────────────────────────────────────────┘

1. alert-service (알람 생성 및 관리)
   ├── 책임: 알람 생성, 조회, 상태 관리
   ├── 데이터: alerts 테이블
   ├── 의존성: 
   │   ├── thresholds-service (임계치 정보)
   │   ├── location-service (위치 정보)
   │   └── sensor-threshold-mapping-service (매핑 정보)
   └── 포트: 30006

2. alert-subscription-service (구독 관리)
   ├── 책임: 구독 CRUD, 구독자별 필터링
   ├── 데이터: alert_subscriptions 테이블
   ├── 의존성: location-service (위치 계층 구조)
   └── 포트: 30007

3. alert-notification-service (알림 발송)
   ├── 책임: 알림 발송, 발송 이력 관리
   ├── 데이터: alert_notifications 테이블
   ├── 의존성:
   │   ├── alert-service (알람 정보)
   │   └── alert-subscription-service (구독 정보)
   └── 포트: 30008

4. sensor-threshold-mapping-service (센서-임계치 매핑)
   ├── 책임: 센서별 임계치 매핑 관리
   ├── 데이터: sensor_threshold_map 테이블
   ├── 의존성:
   │   ├── thresholds-service (임계치 정보)
   │   └── location-service (센서 정보)
   └── 포트: 30009

5. alert-evaluation-service (임계치 검증 워커) ⭐ NEW
   ├── 책임: 백그라운드에서 지속적으로 임계치 초과 감지
   ├── 데이터: (읽기 전용) temperature_raw
   ├── 실행 방식: 
   │   ├── 스케줄러 기반 (주기적 실행, 예: 1분마다)
   │   └── 또는 이벤트 기반 (ETL 완료 후 트리거)
   ├── 의존성:
   │   ├── sensor-threshold-mapping-service (매핑 정보)
   │   ├── thresholds-service (임계치 정보)
   │   ├── location-service (위치 정보)
   │   └── alert-service (알람 생성)
   └── 포트: 30010 (내부 서비스, 외부 노출 불필요)
```

---

## 🎯 세분화 + alert-evaluation-service

### 이유
1. **단일 책임 원칙**: 각 서비스가 명확한 책임
2. **독립적 확장**: 알림 발송량이 많을 경우 notification-service만 스케일
3. **장애 격리**: 구독 관리 문제가 알림 발송에 영향 없음
4. **팀 분리**: 각 서비스를 다른 팀이 담당 가능
5. **실시간 감지**: API 호출과 무관하게 백그라운드에서 지속적으로 임계치 검증 ⭐

---

## 📐 서비스별 상세 설계

### 1. alert-service

**API 엔드포인트:**
```
POST   /api/v1/alerts                    # 알람 생성
GET    /api/v1/alerts                    # 알람 목록 조회
GET    /api/v1/alerts/{alert_id}         # 알람 상세 조회
GET    /api/v1/alerts/by-sensor/{sensor_id}  # 센서별 알람 조회
GET    /api/v1/alerts/by-location/{loc_id}   # 위치별 알람 조회
PUT    /api/v1/alerts/{alert_id}/resolve # 알람 해결 처리
```

**서비스 간 통신:**
```python
# sensor-threshold-mapping-service 호출
GET /api/v1/mappings/sensor/{sensor_id}
→ 센서에 적용된 임계치 매핑 조회

# thresholds-service 호출
GET /api/v1/thresholds/{threshold_id}
→ 임계치 상세 정보 조회

# location-service 호출
GET /api/v1/location/{sensor_id}
→ 센서의 위치 정보 조회
```

**알람 생성 로직:**
```python
# ❌ 기존 방식 (비권장): realtime-service API 호출 시마다 체크
# ✅ 새로운 방식: alert-evaluation-service가 백그라운드에서 지속적으로 검증

# alert-evaluation-service 내부 로직
async def evaluate_thresholds():
    # 1. 최신 센서 데이터 조회 (temperature_raw)
    latest_data = await get_latest_temperature_data()
    
    # 2. 각 센서별로 임계치 매핑 조회
    for sensor_data in latest_data:
        mappings = await sensor_threshold_mapping_client.get_active_mappings(
            sensor_id=sensor_data.sensor_id
        )
        
        # 3. 임계치 초과 여부 확인
        for mapping in mappings:
            threshold = await thresholds_client.get_threshold(mapping.threshold_id)
            if is_threshold_exceeded(sensor_data.value, threshold):
                # 4. 알람 생성
                await alert_service_client.create_alert({
                    "sensor_id": sensor_data.sensor_id,
                    "threshold_type": threshold.threshold_type,
                    "threshold_level": threshold.level,
                    "measured_value": sensor_data.value,
                    "threshold_id": threshold.threshold_id,
                    "threshold_map_id": mapping.map_id
                })
```

---

### 2. alert-subscription-service

**API 엔드포인트:**
```
POST   /api/v1/subscriptions             # 구독 생성
GET    /api/v1/subscriptions             # 구독 목록 조회
GET    /api/v1/subscriptions/{subscription_id}  # 구독 상세
PUT    /api/v1/subscriptions/{subscription_id}   # 구독 수정
DELETE /api/v1/subscriptions/{subscription_id}  # 구독 삭제

# 위치 기반 구독 조회 (핵심 기능)
GET    /api/v1/subscriptions/match       # 위치 매칭 구독 조회
       ?factory=SinPyeong
       &building=F-2001
       &floor=1
       &area=조립2
```

**위치 매칭 로직:**
```python
# factory만 지정 → 해당 factory 전체 구독
GET /api/v1/subscriptions/match?factory=SinPyeong

# factory+building → 특정 building만
GET /api/v1/subscriptions/match?factory=SinPyeong&building=F-2001

# factory+building+floor → 특정 층만
GET /api/v1/subscriptions/match?factory=SinPyeong&building=F-2001&floor=1

# factory+building+floor+area → 특정 구역만
GET /api/v1/subscriptions/match?factory=SinPyeong&building=F-2001&floor=1&area=조립2
```

**매칭 알고리즘:**
```sql
-- 구독 조건이 알람 위치와 매칭되는지 확인
SELECT * FROM alert_subscriptions
WHERE enabled = true
  AND (
    (factory IS NULL OR factory = :factory)
    AND (building IS NULL OR building = :building)
    AND (floor IS NULL OR floor = :floor)
    AND (area IS NULL OR area = :area)
  )
  AND (sensor_id IS NULL OR sensor_id = :sensor_id)
  AND (threshold_type IS NULL OR threshold_type = :threshold_type)
  AND (min_level IS NULL OR min_level <= :alert_level)
```

---

### 3. alert-notification-service

**API 엔드포인트:**
```
POST   /api/v1/notifications/send        # 알림 발송 요청
GET    /api/v1/notifications             # 발송 이력 조회
GET    /api/v1/notifications/{notification_id}  # 발송 상세
GET    /api/v1/notifications/by-alert/{alert_id}  # 알람별 발송 이력
PUT    /api/v1/notifications/{notification_id}/retry  # 재시도
```

**서비스 간 통신:**
```python
# alert-service 호출
GET /api/v1/alerts/{alert_id}
→ 알람 정보 조회

# alert-subscription-service 호출
GET /api/v1/subscriptions/match?factory=...&building=...
→ 해당 위치의 구독자 목록 조회
```

**알림 발송 플로우:**
```
1. alert-service에서 알람 생성
2. alert-service가 notification-service에 발송 요청
   POST /api/v1/notifications/send
   {
       "alert_id": 123,
       "subscription_ids": [1, 2, 3]
   }
3. notification-service가 이메일/SMS 발송
4. 발송 결과를 alert_notifications 테이블에 저장
```

---

### 4. sensor-threshold-mapping-service

**API 엔드포인트:**
```
POST   /api/v1/mappings                 # 매핑 생성
GET    /api/v1/mappings                 # 매핑 목록 조회
GET    /api/v1/mappings/sensor/{sensor_id}  # 센서별 매핑 조회
GET    /api/v1/mappings/threshold/{threshold_id}  # 임계치별 매핑
PUT    /api/v1/mappings/{map_id}         # 매핑 수정
DELETE /api/v1/mappings/{map_id}         # 매핑 삭제
GET    /api/v1/mappings/active/sensor/{sensor_id}  # 활성 매핑 조회
```

**서비스 간 통신:**
```python
# thresholds-service 호출
GET /api/v1/thresholds/{threshold_id}
→ 임계치 상세 정보

# location-service 호출
GET /api/v1/location/{sensor_id}
→ 센서 위치 정보
```

**활성 매핑 조회 로직:**
```sql
SELECT * FROM sensor_threshold_map
WHERE sensor_id = :sensor_id
  AND enabled = true
  AND (effective_from IS NULL OR effective_from <= NOW())
  AND (effective_to IS NULL OR effective_to >= NOW())
ORDER BY threshold_id
```

**스키마 수정 사항:**
```sql
-- ❌ 기존 (시간 단위만 표현 가능)
duration_hours int4 DEFAULT 1 NOT NULL

-- ✅ 수정 (초/분/시 모두 표현 가능)
duration_seconds int4 DEFAULT 60 NOT NULL  -- 기본값: 60초 (1분)

-- 사용 예시:
-- 1초 = 1
-- 1분 = 60
-- 10분 = 600
-- 1시간 = 3600
-- 24시간 = 86400
```

**duration_seconds의 의미:**
- 임계치 초과가 **지속되어야 알람을 발생시킬 최소 시간** (초 단위)
- 예: `duration_seconds = 300` (5분)인 경우, 임계치 초과가 5분 이상 지속되어야 알람 발생
- 중복 알람 방지 및 노이즈 필터링에 사용

---

### 5. alert-evaluation-service (임계치 검증 워커)

**역할:**
- 백그라운드에서 지속적으로 temperature_raw 데이터를 스캔
- 센서별 임계치 초과 여부 검증
- 임계치 초과 시 alert-service에 알람 생성 요청

**실행 방식:**
- **스케줄러 기반**: APScheduler 또는 Celery Beat 사용
- **실행 주기**: 1분마다 (설정 가능)
- **중복 실행 방지**: max_instances=1

**API 엔드포인트 (선택사항 - 모니터링용):**
```
GET    /health                    # 헬스체크
GET    /status                    # 워커 상태 조회
POST   /evaluate/trigger          # 수동 트리거 (테스트용)
GET    /metrics                   # 메트릭 (처리된 레코드 수 등)
```

**서비스 간 통신:**
```python
# sensor-threshold-mapping-service 호출
GET /api/v1/mappings/active/sensor/{sensor_id}
→ 센서에 적용된 활성 임계치 매핑 조회

# thresholds-service 호출
GET /api/v1/thresholds/{threshold_id}
→ 임계치 상세 정보 조회

# location-service 호출
GET /api/v1/location/{sensor_id}
→ 센서의 위치 정보 조회

# alert-service 호출
POST /api/v1/alerts
→ 알람 생성 요청
```

**핵심 로직:**
```python
async def evaluate_thresholds():
    """임계치 검증 메인 로직"""
    # 1. 최근 처리 시간 이후의 새 데이터 조회
    last_check_time = await get_last_check_time()
    new_data = await db.query(
        "SELECT * FROM flet_montrg.temperature_raw "
        "WHERE capture_dt > :last_check_time "
        "ORDER BY capture_dt DESC",
        last_check_time=last_check_time
    )
    
    # 2. 센서별로 그룹화하여 최신 값만 사용
    sensor_latest = {}
    for row in new_data:
        if row.sensor_id not in sensor_latest:
            sensor_latest[row.sensor_id] = row
    
    # 3. 각 센서별 임계치 검증
    for sensor_id, data in sensor_latest.items():
        # 센서별 활성 매핑 조회
        mappings = await mapping_client.get_active_mappings(sensor_id)
        
        for mapping in mappings:
            threshold = await thresholds_client.get_threshold(mapping.threshold_id)
            
            # 임계치 타입에 맞는 값 추출
            value = extract_value_by_type(data, threshold.threshold_type)
            
            # 임계치 초과 확인
            if is_exceeded(value, threshold):
                # duration_seconds 체크: 임계치 초과가 지속 시간 이상인지 확인
                if await check_duration_exceeded(
                    sensor_id, 
                    mapping.threshold_id, 
                    mapping.duration_seconds,
                    data.capture_dt
                ):
                    # 알람 생성
                    await alert_client.create_alert({
                        "sensor_id": sensor_id,
                        "loc_id": data.loc_id,
                        "threshold_type": threshold.threshold_type,
                        "threshold_level": threshold.level,
                        "measured_value": value,
                        "threshold_id": threshold.threshold_id,
                        "threshold_map_id": mapping.map_id,
                        "alert_time": data.capture_dt
                    })
    
    # 4. 마지막 처리 시간 업데이트
    await update_last_check_time(datetime.now())
```

**duration_seconds 기반 지속 시간 체크:**
```python
async def check_duration_exceeded(
    sensor_id: str, 
    threshold_id: int, 
    duration_seconds: int,
    current_time: datetime
) -> bool:
    """
    임계치 초과가 duration_seconds 이상 지속되었는지 확인
    
    로직:
    1. temperature_raw에서 최근 duration_seconds 동안의 데이터 조회
    2. 모든 데이터가 임계치 초과 상태인지 확인
    3. 모두 초과 상태면 True 반환
    """
    # duration_seconds 이전 시간 계산
    start_time = current_time - timedelta(seconds=duration_seconds)
    
    # 최근 duration_seconds 동안의 데이터 조회
    recent_data = await db.query(
        """
        SELECT * FROM flet_montrg.temperature_raw
        WHERE sensor_id = :sensor_id
          AND capture_dt >= :start_time
          AND capture_dt <= :current_time
        ORDER BY capture_dt ASC
        """,
        sensor_id=sensor_id,
        start_time=start_time,
        current_time=current_time
    )
    
    if not recent_data:
        return False  # 데이터가 없으면 알람 생성 안 함
    
    # 임계치 정보 조회
    threshold = await thresholds_client.get_threshold(threshold_id)
    
    # 모든 데이터가 임계치 초과 상태인지 확인
    for data in recent_data:
        value = extract_value_by_type(data, threshold.threshold_type)
        if not is_exceeded(value, threshold):
            return False  # 하나라도 정상 범위면 알람 생성 안 함
    
    # duration_seconds 동안 모두 초과 상태였음
    return True

async def should_create_alert(
    sensor_id: str, 
    threshold_id: int, 
    alert_time: datetime
) -> bool:
    """
    중복 알람 방지: 동일한 센서+임계치 조합에 대해 
    일정 시간 내 중복 알람 방지
    """
    last_alert = await alert_client.get_latest_alert(
        sensor_id=sensor_id,
        threshold_id=threshold_id
    )
    
    if last_alert:
        # 마지막 알람 이후 5분 이내면 중복 방지
        time_diff = alert_time - last_alert.alert_time
        if time_diff.total_seconds() < 300:  # 5분
            return False
    
    return True
```

---

## 🔄 서비스 간 통신 플로우

### 알람 발생 전체 플로우 (개선된 버전)

```
[데이터 수집]
Airflow ETL (매 10분마다)
    │
    └─→ temperature_raw 테이블에 데이터 적재
    │
    ▼
[임계치 검증 - 백그라운드 워커]
[0] alert-evaluation-service (스케줄러/워커)
    │
    ├─→ temperature_raw 최신 데이터 조회 (읽기 전용)
    ├─→ sensor-threshold-mapping-service: 센서별 활성 매핑 조회
    │   GET /api/v1/mappings/active/sensor/{sensor_id}
    ├─→ thresholds-service: 임계치 상세 정보 조회
    │   GET /api/v1/thresholds/{threshold_id}
    └─→ location-service: 센서 위치 정보 조회
        GET /api/v1/location/{sensor_id}
    │
    ▼ 임계치 초과 감지
    │
[1] alert-service
    │
    ├─→ POST /api/v1/alerts (알람 생성)
    ├─→ alert-subscription-service: 위치 기반 구독 조회
    │   GET /api/v1/subscriptions/match?factory=...&building=...
    └─→ alert-notification-service: 알림 발송 요청
        POST /api/v1/notifications/send
        │
[2] alert-notification-service
    │
    ├─→ 구독자별 알림 발송 (이메일/SMS)
    └─→ alert_notifications 테이블에 발송 이력 저장
```

### alert-evaluation-service 실행 방식

**옵션 1: 스케줄러 기반 (권장)**
```python
# FastAPI + APScheduler 또는 Celery Beat
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()
scheduler.add_job(
    evaluate_thresholds,
    'interval',
    minutes=1,  # 1분마다 실행
    max_instances=1  # 중복 실행 방지
)
scheduler.start()
```

**옵션 2: 이벤트 기반**
```python
# Airflow ETL 완료 후 webhook 호출
# 또는 데이터베이스 트리거 사용
# 또는 메시지 큐 (RabbitMQ, Kafka) 사용
```

**옵션 3: Kubernetes CronJob**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: alert-evaluation
spec:
  schedule: "*/1 * * * *"  # 매 1분마다
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: alert-evaluation
            image: flet-montrg/alert-evaluation-service:latest
            command: ["python", "evaluate_thresholds.py"]
```

---

## 📦 데이터베이스 소유권 분리

### 서비스별 데이터 소유권

| 서비스 | 소유 테이블 | 접근 권한 |
|--------|------------|----------|
| **alert-service** | `alerts` | 읽기/쓰기 전용 |
| **alert-subscription-service** | `alert_subscriptions` | 읽기/쓰기 전용 |
| **alert-notification-service** | `alert_notifications` | 읽기/쓰기 전용 |
| **sensor-threshold-mapping-service** | `sensor_threshold_map` | 읽기/쓰기 전용 |
| **alert-evaluation-service** | (없음) | 읽기 전용: `temperature_raw` |
| **thresholds-service** | `thresholds` | 읽기/쓰기 전용 |
| **location-service** | `locations`, `sensors` | 읽기/쓰기 전용 |
| **realtime-service** | (없음) | 읽기 전용: `temperature_raw` |
| **aggregation-service** | (없음) | 읽기 전용: `temperature_raw` |

**원칙:**
- 각 서비스는 자신의 테이블에만 쓰기 권한
- 다른 서비스의 테이블은 HTTP API를 통해서만 읽기
- 데이터 일관성은 서비스 간 통신으로 보장

---

## 🚀 구현 단계

### Phase 1: 핵심 서비스 구축 (1-2주)
1. **sensor-threshold-mapping-service** 구현
   - 센서-임계치 매핑 CRUD
   - 활성 매핑 조회 API
   - 기존 데이터 마이그레이션

2. **alert-service** 기본 구현
   - 알람 생성 API
   - 알람 조회 API
   - sensor-threshold-mapping-service 통합

### Phase 2: 구독 및 알림 (2-3주)
3. **alert-subscription-service** 구현
   - 구독 CRUD API
   - 위치 기반 매칭 로직
   - location-service 통합

4. **alert-notification-service** 구현
   - 알림 발송 엔진 (이메일/SMS)
   - 발송 이력 관리
   - 재시도 로직

### Phase 3: 임계치 검증 워커 (1-2주)
5. **alert-evaluation-service** 구현
   - 백그라운드 스케줄러/워커 구현
   - temperature_raw 데이터 스캔 로직
   - 임계치 초과 감지 알고리즘
   - alert-service 통합

### Phase 4: 통합 및 최적화 (1-2주)
6. **realtime-service** 정리
   - 임계치 검증 로직 제거 (alert-evaluation-service로 이관)
   - API는 조회 전용으로 단순화

7. **모니터링 및 최적화**
   - 각 서비스 모니터링 설정
   - 성능 튜닝
   - 에러 핸들링 강화
   - alert-evaluation-service 실행 주기 최적화

---

## 🔧 기술 스택 (기존과 동일)

- **Backend**: Python/FastAPI
- **Database**: PostgreSQL (각 서비스별 스키마 분리)
- **Container**: Docker
- **Orchestration**: Kubernetes (Kind)
- **Service Communication**: HTTP/REST (httpx.AsyncClient)
- **API Documentation**: OpenAPI/Swagger

---

## 📊 Kubernetes 배포 구조

```
flet_montrg/
├── k8s/
│   ├── alert/                    # alert-service
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   ├── configmap.yaml
│   │   ├── secret.yaml
│   │   ├── hpa.yaml
│   │   └── network-policy.yaml
│   ├── alert-subscription/       # alert-subscription-service
│   ├── alert-notification/       # alert-notification-service
│   ├── sensor-threshold-mapping/ # sensor-threshold-mapping-service
│   └── alert-evaluation/         # alert-evaluation-service (워커)
```

---

## 🎯 장점

1. **확장성**: 알림 발송량이 많을 경우 notification-service만 스케일
2. **유지보수성**: 각 서비스가 독립적으로 개발/배포 가능
3. **장애 격리**: 한 서비스 장애가 다른 서비스에 영향 최소화
4. **팀 분리**: 각 서비스를 다른 팀이 담당 가능
5. **테스트 용이성**: 각 서비스를 독립적으로 테스트 가능

---

## ⚠️ 고려사항

1. **분산 트랜잭션**: 알람 생성과 알림 발송의 일관성 보장 필요
   - 해결: 이벤트 기반 아키텍처 또는 Saga 패턴 고려

2. **서비스 간 의존성**: 순환 의존성 방지
   - 해결: 단방향 의존성 유지 (realtime → alert → notification)

3. **데이터 일관성**: 여러 서비스에 걸친 데이터 일관성
   - 해결: 이벤트 소싱 또는 최종 일관성(Eventual Consistency) 수용

4. **성능**: 서비스 간 HTTP 호출 오버헤드
   - 해결: 비동기 처리, 캐싱, 배치 처리

---

## 📝 다음 단계

1. 각 서비스의 상세 API 스펙 작성
2. 데이터베이스 스키마 최종 확정 및 마이그레이션
3. 서비스 간 통신 프로토콜 정의
4. 에러 핸들링 및 재시도 전략 수립
5. 모니터링 및 로깅 전략 수립

---

## 🔧 데이터베이스 스키마 수정 사항

### sensor_threshold_map 테이블 수정

**변경 사항:**
```sql
-- 기존 컬럼 제거
ALTER TABLE flet_montrg.sensor_threshold_map 
DROP COLUMN IF EXISTS duration_hours;

-- 새 컬럼 추가 (초 단위)
ALTER TABLE flet_montrg.sensor_threshold_map 
ADD COLUMN duration_seconds int4 DEFAULT 60 NOT NULL;

-- 기존 데이터 마이그레이션 (시간 → 초 변환)
UPDATE flet_montrg.sensor_threshold_map 
SET duration_seconds = duration_hours * 3600 
WHERE duration_hours IS NOT NULL;

-- 인덱스는 그대로 유지 (sensor_id, threshold_id 기반)
```

**수정된 스키마:**
```sql
CREATE TABLE flet_montrg.sensor_threshold_map (
    map_id bigserial NOT NULL,
    sensor_id varchar(50) NOT NULL,
    threshold_id int4 NOT NULL,
    duration_seconds int4 DEFAULT 60 NOT NULL,  -- ⭐ 수정: 초 단위
    enabled bool DEFAULT true NOT NULL,
    effective_from timestamptz NULL,
    effective_to timestamptz NULL,
    upd_dt timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT sensor_threshold_map_pkey PRIMARY KEY (map_id),
    CONSTRAINT sensor_threshold_map_sensor_threshold_uk UNIQUE (sensor_id, threshold_id),
    CONSTRAINT sensor_threshold_map_sensor_fkey FOREIGN KEY (sensor_id) 
        REFERENCES flet_montrg.sensors(sensor_id),
    CONSTRAINT sensor_threshold_map_threshold_fkey FOREIGN KEY (threshold_id) 
        REFERENCES flet_montrg.thresholds(threshold_id)
);

-- 인덱스
CREATE INDEX idx_stm_effective ON flet_montrg.sensor_threshold_map 
    USING btree (effective_from, effective_to);
CREATE INDEX idx_stm_enabled ON flet_montrg.sensor_threshold_map 
    USING btree (enabled);
CREATE INDEX idx_stm_sensor ON flet_montrg.sensor_threshold_map 
    USING btree (sensor_id);
CREATE INDEX idx_stm_threshold ON flet_montrg.sensor_threshold_map 
    USING btree (threshold_id);
```

**duration_seconds 사용 예시:**
```python
# 1초 = 1
# 1분 = 60
# 5분 = 300
# 10분 = 600
# 30분 = 1800
# 1시간 = 3600
# 24시간 = 86400

# API 요청 예시
POST /api/v1/mappings
{
    "sensor_id": "S001",
    "threshold_id": 123,
    "duration_seconds": 300,  # 5분 동안 지속되어야 알람 발생
    "enabled": true,
    "effective_from": "2025-01-01T00:00:00Z",
    "effective_to": null
}
```
