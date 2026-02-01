/**
 * Swagger UI 초기화
 */

/**
 * 모든 태그(서비스 카드) 접기
 */
function collapseAllTags() {
    // 여러 번 시도 (DOM이 완전히 렌더링될 때까지 대기)
    let attempts = 0;
    const maxAttempts = 25;
    
    const tryCollapse = () => {
        attempts++;
        
        // Swagger UI의 태그 섹션 찾기 (여러 선택자 시도)
        let tagSections = document.querySelectorAll('.swagger-ui .opblock-tag-section');
        if (tagSections.length === 0) {
            tagSections = document.querySelectorAll('#swagger-ui .opblock-tag-section');
        }
        if (tagSections.length === 0) {
            tagSections = document.querySelectorAll('.opblock-tag-section');
        }
        
        // 태그 헤더 직접 찾기
        let tagHeaders = document.querySelectorAll('.swagger-ui .opblock-tag');
        if (tagHeaders.length === 0) {
            tagHeaders = document.querySelectorAll('#swagger-ui .opblock-tag');
        }
        if (tagHeaders.length === 0) {
            tagHeaders = document.querySelectorAll('.opblock-tag');
        }
        
        if (tagSections.length === 0 && tagHeaders.length === 0 && attempts < maxAttempts) {
            // 아직 DOM이 렌더링되지 않았으면 다시 시도
            setTimeout(tryCollapse, 200);
            return;
        }
        
        if (tagSections.length === 0 && tagHeaders.length === 0) {
            if (attempts >= maxAttempts) {
                console.warn("⚠️ No tag sections or headers found after", maxAttempts, "attempts");
            }
            return;
        }
        
        let collapsedCount = 0;
        
        // 방법 1: 태그 헤더를 직접 클릭하여 접기
        tagHeaders.forEach(header => {
            // is-open 클래스가 있으면 접기
            if (header.classList.contains('is-open') || header.closest('.opblock-tag-section')?.classList.contains('is-open')) {
                // 클릭 이벤트로 접기 (Swagger UI의 기본 동작)
                try {
                    header.click();
                    collapsedCount++;
                } catch (e) {
                    console.warn("Failed to click tag header:", e);
                }
            }
            
            // 강제로 접힌 상태 설정
            header.classList.remove('is-open');
            header.setAttribute('data-collapsed', 'true');
            
            // 화살표 방향 변경
            const arrow = header.querySelector('.arrow');
            if (arrow) {
                arrow.classList.add('right');
                arrow.classList.remove('down');
                arrow.style.transform = 'rotate(-90deg)';
            }
        });
        
        // 방법 2: 태그 섹션 직접 접기
        tagSections.forEach(section => {
            const wasOpen = section.classList.contains('is-open');
            
            if (wasOpen) {
                section.classList.remove('is-open');
                collapsedCount++;
            }
            
            // 접힌 상태로 강제 설정
            section.setAttribute('data-collapsed', 'true');
            section.classList.remove('is-open');
            
            // 내부 opblock들 숨기기 (강제)
            const opblocks = section.querySelectorAll('.opblock');
            opblocks.forEach(opblock => {
                opblock.style.display = 'none';
                opblock.style.visibility = 'hidden';
                opblock.style.height = '0';
                opblock.style.overflow = 'hidden';
                opblock.style.margin = '0';
                opblock.style.padding = '0';
            });
            
            // 태그 섹션 내부의 모든 콘텐츠 숨기기
            const tag = section.querySelector('.opblock-tag');
            if (tag) {
                tag.classList.remove('is-open');
                tag.setAttribute('data-collapsed', 'true');
            }
        });
        
        // 방법 3: 모든 opblock을 직접 숨기기 (강제)
        const allOpblocks = document.querySelectorAll('.swagger-ui .opblock, #swagger-ui .opblock');
        allOpblocks.forEach(opblock => {
            const parentSection = opblock.closest('.opblock-tag-section');
            if (parentSection && parentSection.classList.contains('is-open')) {
                opblock.style.display = 'none';
                opblock.style.visibility = 'hidden';
            }
        });
        
        if (tagSections.length > 0 || tagHeaders.length > 0) {
            console.log(`✅ Processed: ${tagSections.length} sections, ${tagHeaders.length} headers (${collapsedCount} collapsed)`);
        }
    };
    
    // 즉시 시도
    tryCollapse();
}

function initSwaggerUI(specUrl, title = "API Documentation") {
    const ui = SwaggerUIBundle({
        url: specUrl,
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        plugins: [
            SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout",
        // UI 개선: 불필요한 요소들 숨기기
        displayOperationId: false,
        showExtensions: false,
        showCommonExtensions: false,
        tryItOutEnabled: true,
        // 문서 확장 설정: "none" (모두 접힘), "list" (태그만 확장), "full" (모두 확장)
        docExpansion: "none",
        requestInterceptor: createRequestInterceptor(),
        onComplete: () => {
            // 로딩은 모든 초기화 작업이 완료된 후에 숨김
            
            // Swagger UI 로드 완료 후 처리 (에러 처리 강화)
            setTimeout(() => {
                try {
                // information-container의 padding 제거
                removeInformationContainerPadding();
                
                // 헤더 업데이트
                updateDefaultSwaggerHeader();
                console.log("✅ Default Swagger UI header updated");
                
                // 주기적으로 헤더 업데이트
                startHeaderUpdateInterval();
                
                // 불필요한 요소 숨기기
                hideUnwantedElements();
                
                // 모든 태그(서비스 카드) 접기 (여러 번 시도) - 먼저 실행
                collapseAllTags();
                
                // API 섹션 표시 보장 (태그 헤더만, opblock은 제외)
                ensureAPISectionsVisible();
                
                // 추가 시도 (Swagger UI가 나중에 태그를 렌더링할 수 있음)
                setTimeout(() => collapseAllTags(), 300);
                setTimeout(() => collapseAllTags(), 600);
                setTimeout(() => collapseAllTags(), 1000);
                setTimeout(() => collapseAllTags(), 1500);
                setTimeout(() => {
                    collapseAllTags();
                    // 마지막 태그 접기 시도 후 약간의 지연을 두고 로딩 숨기기 준비
                }, 2000);
                
                // Swagger UI의 기본 클릭 동작을 존중하므로 별도의 이벤트 리스너 불필요
                // CSS가 초기 접힌 상태만 처리하고, 클릭 시 Swagger UI가 자동으로 열어줌
                
                // 주기적으로 태그 접기 확인 (초기 로딩 중에만, 사용자가 연 태그는 제외)
                let checkCount = 0;
                const maxChecks = 5; // 체크 횟수 감소
                let loadingHidden = false;
                const collapseInterval = setInterval(() => {
                    checkCount++;
                    // 사용자가 클릭한 태그는 제외하고 접기
                    const openSections = document.querySelectorAll('.swagger-ui .opblock-tag-section.is-open:not([data-user-opened="true"]), #swagger-ui .opblock-tag-section.is-open:not([data-user-opened="true"])');
                    if (openSections.length > 0 && checkCount <= 3) {
                        // 초기 3번만 접기 시도 (사용자가 아직 클릭하지 않은 경우)
                        console.log(`🔄 Found ${openSections.length} auto-opened sections, collapsing...`);
                        openSections.forEach(section => {
                            const tag = section.querySelector('.opblock-tag');
                            if (tag) {
                                tag.click(); // Swagger UI의 기본 동작으로 접기
                            }
                        });
                    }
                    if (checkCount >= maxChecks) {
                        clearInterval(collapseInterval);
                        console.log("✅ Tag collapse monitoring stopped");
                        
                        // 모든 초기화 작업이 완료된 후 로딩 숨기기
                        if (!loadingHidden) {
                            // 마지막 태그 접기 시도(2000ms) + 모니터링 완료 후 로딩 숨기기
                            setTimeout(() => {
                                hideLoading();
                                loadingHidden = true;
                                console.log("✅ All initialization complete, loading hidden");
                            }, 500);
                        }
                    }
                }, 500);
                
                // 사용자가 태그를 클릭했을 때 표시 (한 번만 설정)
                // 중복 리스너 방지를 위한 플래그
                if (!window._tagClickListenerAttached) {
                    setTimeout(() => {
                        const clickHandler = function(e) {
                            try {
                                const tag = e.target.closest('.opblock-tag');
                                if (tag) {
                                    const section = tag.closest('.opblock-tag-section');
                                    if (section) {
                                        // 사용자가 클릭한 태그로 표시
                                        section.setAttribute('data-user-opened', 'true');
                                    }
                                }
                            } catch (error) {
                                // 에러를 조용히 처리하여 브라우저 확장 프로그램과의 충돌 방지
                                console.debug('Tag click handler error (ignored):', error);
                            }
                        };
                        document.addEventListener('click', clickHandler, true); // capture phase에서 실행
                        window._tagClickListenerAttached = true;
                        window._tagClickHandler = clickHandler; // 나중에 제거할 수 있도록 저장
                    }, 1000);
                }
                
                // 최대 대기 시간 설정 (안전장치) - 3초 후에는 무조건 로딩 숨기기
                setTimeout(() => {
                    if (!loadingHidden) {
                        hideLoading();
                        loadingHidden = true;
                        console.log("✅ Loading hidden after maximum wait time");
                    }
                }, 3000);
                
                console.log("✅ Custom header layout complete");
                
                // 동적 요소 클린업 시작
                startCleanupInterval();
                
                } catch (error) {
                    // 초기화 중 에러 발생 시 조용히 처리 (브라우저 확장 프로그램과의 충돌 방지)
                    console.debug('Swagger UI initialization error (ignored):', error);
                    hideLoading();
                }
            }, UI_CONFIG.initDelay);
        },
        onFailure: (err) => {
            console.error('Swagger UI failed to load:', err);
            try {
                showError('API 스펙을 불러오는데 실패했습니다: ' + (err?.message || String(err)));
            } catch (displayError) {
                console.error('Failed to display error message:', displayError);
            }
            try {
                hideLoading();
            } catch (hideError) {
                console.error('Failed to hide loading:', hideError);
            }
        }
    });
    
    return ui;
}