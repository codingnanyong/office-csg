# Alert 생성 API 예시

## 실제 데이터 기반 예시

### 1. Green 레벨 알람 (정상 범위 내)

```bash
curl -X POST "http://localhost:30007/api/v1/alerts/" \
  -H "Content-Type: application/json" \
  -d '{
    "loc_id": "A011",
    "sensor_id": "TEMPIOT-A011",
    "alert_type": "pcv_temperature",
    "alert_level": "green",
    "threshold_id": 1,
    "threshold_type": "pcv_temperature",
    "threshold_level": "green",
    "measured_value": 25.5,
    "threshold_min": 0.00,
    "threshold_max": 30.90,
    "message": "온도가 정상 범위 내입니다 (자재 보관실)"
  }'
```

### 2. Yellow 레벨 알람 (경고)

```bash
curl -X POST "http://localhost:30007/api/v1/alerts/" \
  -H "Content-Type: application/json" \
  -d '{
    "loc_id": "A015",
    "sensor_id": "TEMPIOT-A015",
    "alert_type": "pcv_temperature",
    "alert_level": "yellow",
    "threshold_id": 2,
    "threshold_type": "pcv_temperature",
    "threshold_level": "yellow",
    "measured_value": 32.0,
    "threshold_min": 31.00,
    "threshold_max": 32.90,
    "message": "온도가 경고 범위입니다 (고주파 작업실)"
  }'
```

### 3. Orange 레벨 알람 (위험)

```bash
curl -X POST "http://localhost:30007/api/v1/alerts/" \
  -H "Content-Type: application/json" \
  -d '{
    "loc_id": "A027",
    "sensor_id": "TEMPIOT-A027",
    "alert_type": "pcv_temperature",
    "alert_level": "orange",
    "threshold_id": 3,
    "threshold_type": "pcv_temperature",
    "threshold_level": "orange",
    "measured_value": 35.5,
    "threshold_min": 33.00,
    "threshold_max": null,
    "message": "온도가 위험 범위를 초과했습니다 (CTM 1호기)"
  }'
```

## Python 예시

```python
import requests
from datetime import datetime

BASE_URL = "http://localhost:30007/api/v1/alerts/"

# 실제 센서 및 위치 데이터
sensors = {
    "A011": {"sensor_id": "TEMPIOT-A011", "area": "자재 보관실"},
    "A015": {"sensor_id": "TEMPIOT-A015", "area": "고주파"},
    "A027": {"sensor_id": "TEMPIOT-A027", "area": "[3P] CTM 1호기"},
    "A034": {"sensor_id": "TEMPIOT-A034", "area": "스프레이"},
}

# Threshold 데이터
thresholds = {
    1: {"type": "pcv_temperature", "level": "green", "min": 0.00, "max": 30.90},
    2: {"type": "pcv_temperature", "level": "yellow", "min": 31.00, "max": 32.90},
    3: {"type": "pcv_temperature", "level": "orange", "min": 33.00, "max": None},
}

def create_alert(loc_id, measured_value, threshold_id):
    """알람 생성"""
    sensor = sensors[loc_id]
    threshold = thresholds[threshold_id]
    
    # 측정값에 따라 적절한 threshold 선택
    if measured_value <= 30.90:
        threshold_id = 1
        alert_level = "green"
    elif measured_value <= 32.90:
        threshold_id = 2
        alert_level = "yellow"
    else:
        threshold_id = 3
        alert_level = "orange"
    
    threshold = thresholds[threshold_id]
    
    data = {
        "loc_id": loc_id,
        "sensor_id": sensor["sensor_id"],
        "alert_type": "pcv_temperature",
        "alert_level": alert_level,
        "threshold_id": threshold_id,
        "threshold_type": threshold["type"],
        "threshold_level": threshold["level"],
        "measured_value": float(measured_value),
        "threshold_min": threshold["min"],
        "threshold_max": threshold["max"],
        "message": f"온도 알람: {sensor['area']} - 측정값: {measured_value}°C"
    }
    
    response = requests.post(BASE_URL, json=data)
    return response.json()

# 예시 사용
# create_alert("A011", 25.5, 1)  # Green
# create_alert("A015", 32.0, 2)  # Yellow
# create_alert("A027", 35.5, 3)  # Orange
```

## JavaScript 예시

```javascript
const BASE_URL = "http://localhost:30007/api/v1/alerts/";

const sensors = {
  "A011": { sensor_id: "TEMPIOT-A011", area: "자재 보관실" },
  "A015": { sensor_id: "TEMPIOT-A015", area: "고주파" },
  "A027": { sensor_id: "TEMPIOT-A027", area: "[3P] CTM 1호기" },
  "A034": { sensor_id: "TEMPIOT-A034", area: "스프레이" },
};

const thresholds = {
  1: { type: "pcv_temperature", level: "green", min: 0.00, max: 30.90 },
  2: { type: "pcv_temperature", level: "yellow", min: 31.00, max: 32.90 },
  3: { type: "pcv_temperature", level: "orange", min: 33.00, max: null },
};

function createAlert(locId, measuredValue) {
  let thresholdId, alertLevel;
  
  if (measuredValue <= 30.90) {
    thresholdId = 1;
    alertLevel = "green";
  } else if (measuredValue <= 32.90) {
    thresholdId = 2;
    alertLevel = "yellow";
  } else {
    thresholdId = 3;
    alertLevel = "orange";
  }
  
  const threshold = thresholds[thresholdId];
  const sensor = sensors[locId];
  
  const data = {
    loc_id: locId,
    sensor_id: sensor.sensor_id,
    alert_type: "pcv_temperature",
    alert_level: alertLevel,
    threshold_id: thresholdId,
    threshold_type: threshold.type,
    threshold_level: threshold.level,
    measured_value: measuredValue,
    threshold_min: threshold.min,
    threshold_max: threshold.max,
    message: `온도 알람: ${sensor.area} - 측정값: ${measuredValue}°C`
  };
  
  return fetch(BASE_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(data)
  })
    .then(response => response.json())
    .then(data => {
      console.log("Alert created:", data);
      return data;
    });
}

// 사용 예시
// createAlert("A011", 25.5);  // Green
// createAlert("A015", 32.0);  // Yellow
// createAlert("A027", 35.5);  // Orange
```

## 실제 센서별 예시

### A011 - 자재 보관실 (CSI, SinPyeong, MX-1, 1층)
```json
{
  "loc_id": "A011",
  "sensor_id": "TEMPIOT-A011",
  "alert_type": "pcv_temperature",
  "alert_level": "green",
  "threshold_id": 1,
  "threshold_type": "pcv_temperature",
  "threshold_level": "green",
  "measured_value": 22.5,
  "threshold_min": 0.00,
  "threshold_max": 30.90,
  "message": "자재 보관실 온도 정상"
}
```

### A015 - 고주파 (CSI, SinPyeong, MX-1, 2층)
```json
{
  "loc_id": "A015",
  "sensor_id": "TEMPIOT-A015",
  "alert_type": "pcv_temperature",
  "alert_level": "yellow",
  "threshold_id": 2,
  "threshold_type": "pcv_temperature",
  "threshold_level": "yellow",
  "measured_value": 32.5,
  "threshold_min": 31.00,
  "threshold_max": 32.90,
  "message": "고주파 작업실 온도 경고"
}
```

### A027 - [3P] CTM 1호기 (CSI, SinPyeong, PW Center, 1층)
```json
{
  "loc_id": "A027",
  "sensor_id": "TEMPIOT-A027",
  "alert_type": "pcv_temperature",
  "alert_level": "orange",
  "threshold_id": 3,
  "threshold_type": "pcv_temperature",
  "threshold_level": "orange",
  "measured_value": 35.0,
  "threshold_min": 33.00,
  "threshold_max": null,
  "message": "CTM 1호기 온도 위험"
}
```

### A034 - 스프레이 (CSI, JangNim, Bottom, 1층)
```json
{
  "loc_id": "A034",
  "sensor_id": "TEMPIOT-A034",
  "alert_type": "pcv_temperature",
  "alert_level": "orange",
  "threshold_id": 3,
  "threshold_type": "pcv_temperature",
  "threshold_level": "orange",
  "measured_value": 34.5,
  "threshold_min": 33.00,
  "threshold_max": null,
  "message": "스프레이 작업실 온도 위험"
}
```

## Threshold 기준

- **Green (threshold_id: 1)**: 0.00 ~ 30.90°C
- **Yellow (threshold_id: 2)**: 31.00 ~ 32.90°C  
- **Orange (threshold_id: 3)**: 33.00°C 이상

## Sensor-Threshold 매핑 정보

각 센서는 **Yellow (threshold_id: 2)**와 **Orange (threshold_id: 3)** 두 개의 threshold와 매핑되어 있습니다.

### 센서별 threshold_map_id

| 센서 ID | Yellow (threshold_id: 2) | Orange (threshold_id: 3) |
|---------|-------------------------|-------------------------|
| TEMPIOT-A011 | map_id: 1 | map_id: 2 |
| TEMPIOT-A012 | map_id: 3 | map_id: 4 |
| TEMPIOT-A013 | map_id: 5 | map_id: 6 |
| TEMPIOT-A014 | map_id: 7 | map_id: 8 |
| TEMPIOT-A015 | map_id: 9 | map_id: 10 |
| TEMPIOT-A016 | map_id: 11 | map_id: 12 |
| TEMPIOT-A017 | map_id: 13 | map_id: 14 |
| TEMPIOT-A018 | map_id: 15 | map_id: 16 |
| TEMPIOT-A019 | map_id: 17 | map_id: 18 |
| TEMPIOT-A020 | map_id: 19 | map_id: 20 |
| TEMPIOT-A021 | map_id: 21 | map_id: 22 |
| TEMPIOT-A022 | map_id: 23 | map_id: 24 |
| TEMPIOT-A023 | map_id: 25 | map_id: 26 |
| TEMPIOT-A024 | map_id: 27 | map_id: 28 |
| TEMPIOT-A025 | map_id: 29 | map_id: 30 |
| TEMPIOT-A026 | map_id: 31 | map_id: 32 |
| TEMPIOT-A027 | map_id: 33 | map_id: 34 |
| TEMPIOT-A028 | map_id: 35 | map_id: 36 |
| TEMPIOT-A029 | map_id: 37 | map_id: 38 |
| TEMPIOT-A030 | map_id: 39 | map_id: 40 |
| TEMPIOT-A031 | map_id: 41 | map_id: 42 |
| TEMPIOT-A032 | map_id: 43 | map_id: 44 |
| TEMPIOT-A033 | map_id: 45 | map_id: 46 |
| TEMPIOT-A034 | map_id: 47 | map_id: 48 |
| TEMPIOT-A035 | map_id: 49 | map_id: 50 |
| TEMPIOT-A036 | map_id: 51 | map_id: 52 |
| TEMPIOT-A037 | map_id: 53 | map_id: 54 |
| TEMPIOT-A038 | map_id: 55 | map_id: 56 |

**참고**: 모든 매핑은 `enabled=true` 상태입니다.

## threshold_map_id를 포함한 예시

### Yellow 레벨 알람 (threshold_map_id 포함)

```bash
curl -X POST "http://localhost:30007/api/v1/alerts/" \
  -H "Content-Type: application/json" \
  -d '{
    "loc_id": "A011",
    "sensor_id": "TEMPIOT-A011",
    "alert_type": "pcv_temperature",
    "alert_level": "yellow",
    "threshold_id": 2,
    "threshold_map_id": 1,
    "threshold_type": "pcv_temperature",
    "threshold_level": "yellow",
    "measured_value": 32.0,
    "threshold_min": 31.00,
    "threshold_max": 32.90,
    "message": "온도가 경고 범위입니다 (자재 보관실)"
  }'
```

### Orange 레벨 알람 (threshold_map_id 포함)

```bash
curl -X POST "http://localhost:30007/api/v1/alerts/" \
  -H "Content-Type: application/json" \
  -d '{
    "loc_id": "A011",
    "sensor_id": "TEMPIOT-A011",
    "alert_type": "pcv_temperature",
    "alert_level": "orange",
    "threshold_id": 3,
    "threshold_map_id": 2,
    "threshold_type": "pcv_temperature",
    "threshold_level": "orange",
    "measured_value": 35.5,
    "threshold_min": 33.00,
    "threshold_max": null,
    "message": "온도가 위험 범위를 초과했습니다 (자재 보관실)"
  }'
```

## Python 예시 (threshold_map_id 포함)

```python
import requests

BASE_URL = "http://localhost:30007/api/v1/alerts/"

# Sensor-Threshold 매핑 정보
SENSOR_THRESHOLD_MAP = {
    "TEMPIOT-A011": {"yellow": 1, "orange": 2},
    "TEMPIOT-A012": {"yellow": 3, "orange": 4},
    "TEMPIOT-A013": {"yellow": 5, "orange": 6},
    "TEMPIOT-A014": {"yellow": 7, "orange": 8},
    "TEMPIOT-A015": {"yellow": 9, "orange": 10},
    "TEMPIOT-A016": {"yellow": 11, "orange": 12},
    "TEMPIOT-A017": {"yellow": 13, "orange": 14},
    "TEMPIOT-A018": {"yellow": 15, "orange": 16},
    "TEMPIOT-A019": {"yellow": 17, "orange": 18},
    "TEMPIOT-A020": {"yellow": 19, "orange": 20},
    "TEMPIOT-A021": {"yellow": 21, "orange": 22},
    "TEMPIOT-A022": {"yellow": 23, "orange": 24},
    "TEMPIOT-A023": {"yellow": 25, "orange": 26},
    "TEMPIOT-A024": {"yellow": 27, "orange": 28},
    "TEMPIOT-A025": {"yellow": 29, "orange": 30},
    "TEMPIOT-A026": {"yellow": 31, "orange": 32},
    "TEMPIOT-A027": {"yellow": 33, "orange": 34},
    "TEMPIOT-A028": {"yellow": 35, "orange": 36},
    "TEMPIOT-A029": {"yellow": 37, "orange": 38},
    "TEMPIOT-A030": {"yellow": 39, "orange": 40},
    "TEMPIOT-A031": {"yellow": 41, "orange": 42},
    "TEMPIOT-A032": {"yellow": 43, "orange": 44},
    "TEMPIOT-A033": {"yellow": 45, "orange": 46},
    "TEMPIOT-A034": {"yellow": 47, "orange": 48},
    "TEMPIOT-A035": {"yellow": 49, "orange": 50},
    "TEMPIOT-A036": {"yellow": 51, "orange": 52},
    "TEMPIOT-A037": {"yellow": 53, "orange": 54},
    "TEMPIOT-A038": {"yellow": 55, "orange": 56},
}

def create_alert_with_map(sensor_id, loc_id, measured_value, area_name):
    """threshold_map_id를 포함한 알람 생성"""
    # 측정값에 따라 threshold 선택
    if measured_value <= 30.90:
        threshold_id = 1
        alert_level = "green"
        threshold_map_id = None  # Green은 매핑 없음
    elif measured_value <= 32.90:
        threshold_id = 2
        alert_level = "yellow"
        threshold_map_id = SENSOR_THRESHOLD_MAP[sensor_id]["yellow"]
    else:
        threshold_id = 3
        alert_level = "orange"
        threshold_map_id = SENSOR_THRESHOLD_MAP[sensor_id]["orange"]
    
    thresholds = {
        1: {"type": "pcv_temperature", "level": "green", "min": 0.00, "max": 30.90},
        2: {"type": "pcv_temperature", "level": "yellow", "min": 31.00, "max": 32.90},
        3: {"type": "pcv_temperature", "level": "orange", "min": 33.00, "max": None},
    }
    
    threshold = thresholds[threshold_id]
    
    data = {
        "loc_id": loc_id,
        "sensor_id": sensor_id,
        "alert_type": "pcv_temperature",
        "alert_level": alert_level,
        "threshold_id": threshold_id,
        "threshold_map_id": threshold_map_id,
        "threshold_type": threshold["type"],
        "threshold_level": threshold["level"],
        "measured_value": float(measured_value),
        "threshold_min": threshold["min"],
        "threshold_max": threshold["max"],
        "message": f"온도 알람: {area_name} - 측정값: {measured_value}°C"
    }
    
    response = requests.post(BASE_URL, json=data)
    return response.json()

# 사용 예시
# create_alert_with_map("TEMPIOT-A011", "A011", 32.0, "자재 보관실")  # Yellow
# create_alert_with_map("TEMPIOT-A011", "A011", 35.5, "자재 보관실")  # Orange
```

## 사용 가능한 센서 ID 목록

- TEMPIOT-A011 ~ TEMPIOT-A038
- 각 센서는 loc_id A011 ~ A038과 매핑됨
- 각 센서는 Yellow(threshold_id: 2)와 Orange(threshold_id: 3) threshold와 매핑됨

---

# Notification 생성 API 예시

## POST /api/v1/notifications/ 호출 예시

### 1. cURL 예시

```bash
curl -X POST "http://localhost:30009/api/v1/notifications/" \
  -H "Content-Type: application/json" \
  -d '{
    "alert_id": 1,
    "subscription_id": 1,
    "notify_type": "email",
    "notify_id": "user@example.com"
  }'
```

### 2. Python (requests) 예시

```python
import requests

url = "http://localhost:30009/api/v1/notifications/"
headers = {"Content-Type": "application/json"}

data = {
    "alert_id": 1,
    "subscription_id": 1,
    "notify_type": "email",
    "notify_id": "user@example.com"
}

response = requests.post(url, json=data, headers=headers)
print(response.status_code)
print(response.json())
```

### 3. JavaScript (fetch) 예시

```javascript
const url = "http://localhost:30009/api/v1/notifications/";

const data = {
  alert_id: 1,
  subscription_id: 1,
  notify_type: "email",
  notify_id: "user@example.com"
};

fetch(url, {
  method: "POST",
  headers: {
    "Content-Type": "application/json"
  },
  body: JSON.stringify(data)
})
  .then(response => response.json())
  .then(data => console.log(data))
  .catch(error => console.error("Error:", error));
```

## 알림 타입별 예시

### Email 알림

```json
{
  "alert_id": 1,
  "subscription_id": 1,
  "notify_type": "email",
  "notify_id": "admin@company.com"
}
```

### 카카오톡 알림

```json
{
  "alert_id": 1,
  "subscription_id": 1,
  "notify_type": "kakao",
  "notify_id": "kakao_account_name"
}
```

### SMS 알림

```json
{
  "alert_id": 1,
  "subscription_id": 1,
  "notify_type": "sms",
  "notify_id": "010-1234-5678"
}
```

### 앱 알림

```json
{
  "alert_id": 1,
  "subscription_id": 1,
  "notify_type": "app",
  "notify_id": "app_user_account"
}
```

## 실제 사용 예시

### Alert 생성 후 Notification 생성

```python
import requests

ALERT_URL = "http://localhost:30007/api/v1/alerts/"
NOTIFICATION_URL = "http://localhost:30009/api/v1/notifications/"

# 1. Alert 생성
alert_data = {
    "loc_id": "A011",
    "sensor_id": "TEMPIOT-A011",
    "alert_type": "pcv_temperature",
    "alert_level": "orange",
    "threshold_id": 3,
    "threshold_map_id": 2,
    "threshold_type": "pcv_temperature",
    "threshold_level": "orange",
    "measured_value": 35.5,
    "threshold_min": 33.00,
    "threshold_max": None,
    "message": "온도가 위험 범위를 초과했습니다"
}

alert_response = requests.post(ALERT_URL, json=alert_data)
alert = alert_response.json()
alert_id = alert["alert_id"]

# 2. 해당 Alert에 대한 Notification 생성
notification_data = {
    "alert_id": alert_id,
    "subscription_id": 1,  # 실제 구독 ID
    "notify_type": "email",
    "notify_id": "admin@company.com"
}

notification_response = requests.post(NOTIFICATION_URL, json=notification_data)
notification = notification_response.json()
print(f"Notification created: {notification['notification_id']}")
```

## 응답 예시

```json
{
  "notification_id": 1,
  "alert_id": 1,
  "subscription_id": 1,
  "notify_type": "email",
  "notify_id": "user@example.com",
  "status": "PENDING",
  "try_count": 0,
  "created_time": "2024-01-23T17:00:00+09:00",
  "last_try_time": null,
  "sent_time": null,
  "fail_reason": null
}
```

## 필드 설명

**필수 필드:**
- `alert_id`: 알람 ID (integer) - 생성된 alert의 ID
- `subscription_id`: 구독 ID (integer) - alert_subscriptions 테이블의 subscription_id
- `notify_type`: 알림 타입 (enum: "email", "kakao", "sms", "app")
- `notify_id`: 알림 ID (string)
  - email: 이메일 주소
  - kakao: 카카오톡 계정 이름
  - sms: 전화번호
  - app: 앱 계정 이름

**자동 생성 필드:**
- `notification_id`: 알림 ID (자동 생성)
- `status`: 상태 (기본값: "PENDING")
- `try_count`: 재시도 횟수 (기본값: 0)
- `created_time`: 생성 시간 (KST로 반환)
- `last_try_time`: 마지막 시도 시간 (초기값: null)
- `sent_time`: 발송 시간 (초기값: null)
- `fail_reason`: 실패 사유 (초기값: null)

## 통합 Swagger UI를 통한 테스트

http://localhost:30005/swagger 에서 `alert-notification-service` 섹션의 `/api/v1/notifications/` 엔드포인트를 직접 테스트할 수 있습니다.