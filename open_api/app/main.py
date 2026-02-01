import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.routers.v1.hq.dailystatus import router as dailystatus_v1_router
from app.routers.v1.hq.annual import router as annual_v1_router
from app.routers.v1.hq.wip import router as wip_v1_router
from app.routers.v1.hq.sample import router as sample_v1_router
from app.routers.v1.hq.tag import router as tag_v1_router
from app.routers.v1.hq.iot import router as iot_v1_router
from app.routers.v1.hq.tooling import router as tooling_v1_router

from app.routers.v1.vj.ip_prf import router as ip_rst_v1_router
from app.routers.v1.vj.workorder import router as workorder_v1_router
from app.routers.v1.vj.sparepart import router as sparpart_v1_router

app = FastAPI(
    title='FDW.OpenAPI',
    description=(
        'This is a FastAPI written in Python to utilize data from FDW (Factory Data Warehouse).<br>'
        'Only the APIs mainly used in v1 have been created.'
    ),
    version='1.0.0',
    contact={
        'name': 'Development Team',
        'url': 'https://github.com/your-org/openapi',
        'email': 'dev-team@example.com',
    },
    license_info={
        'name': 'MIT',
        'url': 'https://opensource.org/licenses/MIT',
    },
    docs_url='/docs',
    redoc_url="/redoc",
    openapi_url='/openapi.json',
    swagger_ui_parameters={"syntaxHighlight": {"theme": "obsidian"}}
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          
    allow_credentials=False, 
    allow_methods=["GET"],
    allow_headers=["*"],
)

instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app, include_in_schema=False, should_gzip=True)

CURRENT_DEPLOY = os.getenv('DEPLOY_ENV', 'unknown')

@app.get('/deploy', tags=['Deploy'], summary='Get Deployment Environment', description='Returns the current deployment environment (blue or green).')
async def get_version():
    return {'current_deploy': CURRENT_DEPLOY}

API_VERSIONS = {
    'v1.7': {
        'annual': annual_v1_router,
        'dailystatus': dailystatus_v1_router,
        'wip': wip_v1_router,
        'sample': sample_v1_router,
        'tag':tag_v1_router,
        'iot':iot_v1_router,
        'tooling': tooling_v1_router,
        'ip_prf': ip_rst_v1_router,
        'workorder':workorder_v1_router,
        'sparepart': sparpart_v1_router
    }
}

ROUTER_TAGS = {
    'annual': 'Factory Annual',
    'dailystatus': 'DailyStatus',
    'wip': 'WIP',
    'sample': 'Sample',
    'tag': 'Tag',
    'iot' : 'IoT',
    'tooling' : 'Tooling',
    'ip_prf' : 'IP Annual',
    'workorder' : 'WorkOrder',
    'sparepart' : 'Spare Part'
}

for version, routers in API_VERSIONS.items():
    for route_name, router in routers.items():
        app.include_router(
            router,
            prefix=f'/api/{version}/{route_name}',
            tags=[ROUTER_TAGS.get(route_name, route_name.capitalize())]
        )
