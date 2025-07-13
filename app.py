"""
================================================================================
GEOCODIFICADOR DE CNPJs - ESPÍRITO SANTO - UFES
================================================================================

Desenvolvido por: Leonardo Rocha
Data: Janeiro 2025
Projeto: UFES - Universidade Federal do Espírito Santo

Versão 3.0 - Interface com Abas e Tema UFES

Funcionalidades:
    ABA 1: Processamento de CNPJs - Upload e geocodificação
    ABA 2: Visualização de Dados - Upload de arquivos processados e mapa

================================================================================
"""

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
import geopandas as gpd
import os
from geopy.geocoders import Nominatim, ArcGIS, GoogleV3, Photon
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
import time
import json
from shapely.geometry import Point
import zipfile
import tempfile
from pathlib import Path
import base64
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import hashlib
from typing import Optional, Tuple, Dict, Any
import datetime
from io import StringIO, BytesIO
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('geocoding.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cache global para geocodificação
GEOCODING_CACHE = {}
CACHE_FILE = "geocoding_cache.pkl"

# Estados do processamento
PROCESSING_STATES = {
    'IDLE': 'idle',
    'RUNNING': 'running',
    'PAUSED': 'paused',
    'STOPPED': 'stopped',
    'COMPLETED': 'completed'
}



# Classe para capturar logs
class LogCapture:
    def __init__(self):
        self.logs = []
        self.max_logs = 100
    
    def add_log(self, level, message, details=None):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'message': message,
            'details': details or ""
        }
        self.logs.append(log_entry)
        
        # Manter apenas os últimos logs
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)
    
    def get_logs(self):
        return self.logs
    
    def clear_logs(self):
        self.logs = []

# Instância global do capturador de logs
log_capture = LogCapture()

# Função para exibir logs em tempo real
def display_logs(container_key="default"):
    """Exibe logs em tempo real de forma amigável"""
    logs = log_capture.get_logs()
    
    if logs:
        st.markdown("### 📋 Log de Execução")
        
        # Criar texto formatado dos logs
        log_text = ""
        for log in logs[-15:]:  # Mostrar apenas os últimos 15 logs para melhor performance
            emoji = {
                'INFO': '🔵',
                'SUCCESS': '✅',
                'WARNING': '⚠️',
                'ERROR': '❌'
            }.get(log['level'], '📝')
            
            log_text += f"{emoji} **{log['timestamp']}** - {log['message']}\n"
            if log['details']:
                log_text += f"   *{log['details']}*\n"
            log_text += "\n"
        
        # Mostrar em um text area com scroll usando key única
        st.text_area(
            "Logs do Sistema",
            value=log_text,
            height=250,
            key=f"log_display_{container_key}",
            help="Últimos 15 logs do processamento",
            disabled=True
        )

# Função para salvar progresso parcial
def save_partial_progress(df, processed_count, total_count):
    """Salva progresso parcial no session state"""
    if 'partial_results' not in st.session_state:
        st.session_state['partial_results'] = {}
    
    st.session_state['partial_results'] = {
        'df': df.copy(),
        'processed_count': processed_count,
        'total_count': total_count,
        'timestamp': datetime.datetime.now().isoformat()
    }
    
    log_capture.add_log('INFO', f"Progresso salvo: {processed_count}/{total_count} registros")

# Função para criar botões de controle
def create_control_buttons():
    """Cria botões de controle do processamento"""
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    current_state = st.session_state.get('processing_state', PROCESSING_STATES['IDLE'])
    
    with col1:
        if current_state == PROCESSING_STATES['IDLE']:
            start_clicked = st.button("🚀 Iniciar Processamento", type="primary", use_container_width=True)
        elif current_state == PROCESSING_STATES['RUNNING']:
            start_clicked = st.button("🟢 Processando...", disabled=True, use_container_width=True)
        elif current_state == PROCESSING_STATES['PAUSED']:
            start_clicked = st.button("🟡 Pausado", disabled=True, use_container_width=True)
        elif current_state == PROCESSING_STATES['STOPPED']:
            start_clicked = st.button("🔴 Parado", disabled=True, use_container_width=True)
        else:
            start_clicked = st.button("✅ Concluído", disabled=True, use_container_width=True)
    
    with col2:
        if current_state == PROCESSING_STATES['RUNNING']:
            pause_clicked = st.button("⏸️ Pausar", use_container_width=True)
        elif current_state == PROCESSING_STATES['PAUSED']:
            pause_clicked = st.button("▶️ Retomar", use_container_width=True)
        else:
            pause_clicked = st.button("⏸️ Pausar", disabled=True, use_container_width=True)
    
    with col3:
        if current_state in [PROCESSING_STATES['RUNNING'], PROCESSING_STATES['PAUSED']]:
            stop_clicked = st.button("⏹️ Parar", use_container_width=True)
        else:
            stop_clicked = st.button("⏹️ Parar", disabled=True, use_container_width=True)
    
    with col4:
        if 'partial_results' in st.session_state and st.session_state['partial_results'].get('processed_count', 0) > 0:
            download_clicked = st.button("📥 Baixar Parcial", use_container_width=True)
        else:
            download_clicked = st.button("📥 Baixar Parcial", disabled=True, use_container_width=True)
    
    return start_clicked, pause_clicked, stop_clicked, download_clicked

# Função melhorada para geocodificação com controle
def geocode_batch_with_control(df: pd.DataFrame, geocoders: list, batch_size: int = 10) -> pd.DataFrame:
    """
    Geocodifica DataFrame em lotes com controle de pause/stop
    """
    
    # Inicializar estado se não existir
    if 'processing_state' not in st.session_state:
        st.session_state['processing_state'] = PROCESSING_STATES['IDLE']
    
    if 'processed_index' not in st.session_state:
        st.session_state['processed_index'] = 0
    
    # Carregar cache
    load_cache()
    log_capture.add_log('INFO', "Sistema iniciado", f"Cache carregado com {len(GEOCODING_CACHE)} entradas")
    
    # Configurar DataFrame
    if st.session_state['processed_index'] == 0:
        df['latitude'] = None
        df['longitude'] = None
        df['geocoding_method'] = None
        df['geocoding_status'] = None
        log_capture.add_log('INFO', "DataFrame configurado", f"Total de {len(df)} registros para processar")
    
    # Obter containers globais da interface principal
    progress_container = st.session_state.get('progress_container', st.empty())
    stats_container = st.session_state.get('stats_container', st.empty())
    log_container = st.session_state.get('log_container', st.empty())
    
    # Estatísticas
    total_records = len(df)
    start_index = st.session_state['processed_index']
    success_count = 0
    cep_count = 0
    error_count = 0
    cache_count = 0
    
    # Contar sucessos existentes
    if start_index > 0:
        success_count = len(df.iloc[:start_index][df.iloc[:start_index]['geocoding_status'] == 'Sucesso'])
        cep_count = len(df.iloc[:start_index][df.iloc[:start_index]['geocoding_method'].str.contains('CEP', na=False)])
        cache_count = len(df.iloc[:start_index][df.iloc[:start_index]['geocoding_status'] == 'Cache'])
        error_count = start_index - success_count - cep_count - cache_count
        log_capture.add_log('INFO', f"Retomando processamento do índice {start_index}")
    
    # Atualizar estado
    st.session_state['processing_state'] = PROCESSING_STATES['RUNNING']
    
    # Progress bar
    progress_bar = None
    status_text = None
    
    # Processar registros
    for i in range(start_index, len(df)):
        # Verificar estado de controle
        current_state = st.session_state.get('processing_state', PROCESSING_STATES['RUNNING'])
        
        if current_state == PROCESSING_STATES['STOPPED']:
            log_capture.add_log('WARNING', "Processamento interrompido pelo usuário")
            break
        
        if current_state == PROCESSING_STATES['PAUSED']:
            log_capture.add_log('INFO', "Processamento pausado")
            while st.session_state.get('processing_state') == PROCESSING_STATES['PAUSED']:
                time.sleep(0.1)
            
            if st.session_state.get('processing_state') == PROCESSING_STATES['STOPPED']:
                log_capture.add_log('WARNING', "Processamento interrompido durante pausa")
                break
            
            log_capture.add_log('INFO', "Processamento retomado")
        
        current_idx = i
        row = df.iloc[i]
        
        # Atualizar progresso em tempo real
        progress = (current_idx + 1) / total_records
        
        # Atualizar progress bar e status
        if progress_bar is None:
            with progress_container.container():
                progress_bar = st.progress(progress)
                status_text = st.empty()
        else:
            progress_bar.progress(progress)
            if status_text is not None:
                status_text.text(f"Processando {current_idx + 1}/{total_records} registros...")
        
        # Salvar progresso a cada 25 registros
        if (current_idx + 1) % 25 == 0:
            save_partial_progress(df, current_idx + 1, total_records)
            st.session_state['processed_index'] = current_idx + 1
        
        # Log do endereço sendo processado
        endereco = row.get('endereco_completo', 'Sem endereço')
        log_capture.add_log('INFO', f"Processando registro {current_idx + 1}", f"Endereço: {endereco[:50]}...")
        
        # Tentar geocodificar por endereço completo
        if pd.notna(row['endereco_completo']):
            try:
                lat, lon, method, status = geocode_address_robust(row['endereco_completo'], geocoders)
                
                if lat is not None:
                    df.iloc[i, df.columns.get_loc('latitude')] = lat
                    df.iloc[i, df.columns.get_loc('longitude')] = lon
                    df.iloc[i, df.columns.get_loc('geocoding_method')] = method
                    df.iloc[i, df.columns.get_loc('geocoding_status')] = status
                    
                    if status == "Cache":
                        cache_count += 1
                        log_capture.add_log('SUCCESS', f"Sucesso (Cache) - {method}", f"Coords: {lat:.6f}, {lon:.6f}")
                    else:
                        success_count += 1
                        log_capture.add_log('SUCCESS', f"Sucesso (Endereço) - {method}", f"Coords: {lat:.6f}, {lon:.6f}")
                else:
                    # Tentar por CEP
                    cep_columns = [col for col in df.columns if 'cep' in col.lower() or col == 'V19']
                    if cep_columns:
                        cep_value = row[cep_columns[0]]
                        log_capture.add_log('INFO', f"Tentando geocodificar por CEP", f"CEP: {cep_value}")
                        
                        lat, lon, method, status = geocode_cep_robust(cep_value, geocoders)
                        
                        if lat is not None:
                            df.iloc[i, df.columns.get_loc('latitude')] = lat
                            df.iloc[i, df.columns.get_loc('longitude')] = lon
                            df.iloc[i, df.columns.get_loc('geocoding_method')] = method
                            df.iloc[i, df.columns.get_loc('geocoding_status')] = status
                            
                            if status == "Cache":
                                cache_count += 1
                                log_capture.add_log('SUCCESS', f"Sucesso (Cache CEP) - {method}", f"Coords: {lat:.6f}, {lon:.6f}")
                            else:
                                cep_count += 1
                                log_capture.add_log('SUCCESS', f"Sucesso (CEP) - {method}", f"Coords: {lat:.6f}, {lon:.6f}")
                        else:
                            df.iloc[i, df.columns.get_loc('geocoding_method')] = method
                            df.iloc[i, df.columns.get_loc('geocoding_status')] = status
                            error_count += 1
                            log_capture.add_log('ERROR', f"Falha na geocodificação", f"Endereço e CEP falharam")
                    else:
                        df.iloc[i, df.columns.get_loc('geocoding_method')] = "Sem CEP"
                        df.iloc[i, df.columns.get_loc('geocoding_status')] = "Falhou"
                        error_count += 1
                        log_capture.add_log('ERROR', f"Falha na geocodificação", f"Sem CEP disponível")
                        
            except Exception as e:
                df.iloc[i, df.columns.get_loc('geocoding_method')] = "Erro"
                df.iloc[i, df.columns.get_loc('geocoding_status')] = "Erro"
                error_count += 1
                log_capture.add_log('ERROR', f"Erro durante geocodificação", f"Erro: {str(e)}")
        else:
            # Tentar apenas por CEP
            cep_columns = [col for col in df.columns if 'cep' in col.lower() or col == 'V19']
            if cep_columns:
                cep_value = row[cep_columns[0]]
                log_capture.add_log('INFO', f"Geocodificando apenas por CEP", f"CEP: {cep_value}")
                
                lat, lon, method, status = geocode_cep_robust(cep_value, geocoders)
                
                if lat is not None:
                    df.iloc[i, df.columns.get_loc('latitude')] = lat
                    df.iloc[i, df.columns.get_loc('longitude')] = lon
                    df.iloc[i, df.columns.get_loc('geocoding_method')] = method
                    df.iloc[i, df.columns.get_loc('geocoding_status')] = status
                    
                    if status == "Cache":
                        cache_count += 1
                        log_capture.add_log('SUCCESS', f"Sucesso (Cache CEP) - {method}", f"Coords: {lat:.6f}, {lon:.6f}")
                    else:
                        cep_count += 1
                        log_capture.add_log('SUCCESS', f"Sucesso (CEP) - {method}", f"Coords: {lat:.6f}, {lon:.6f}")
                else:
                    df.iloc[i, df.columns.get_loc('geocoding_method')] = method
                    df.iloc[i, df.columns.get_loc('geocoding_status')] = status
                    error_count += 1
                    log_capture.add_log('ERROR', f"Falha na geocodificação por CEP", f"CEP: {cep_value}")
            else:
                df.iloc[i, df.columns.get_loc('geocoding_method')] = "Sem CEP"
                df.iloc[i, df.columns.get_loc('geocoding_status')] = "Falhou"
                error_count += 1
                log_capture.add_log('ERROR', f"Sem endereço nem CEP", f"Registro {current_idx + 1} sem dados para geocodificar")
        
        # Atualizar métricas e estatísticas globais em tempo real
        st.session_state['current_stats'] = {
            'success_count': success_count,
            'cep_count': cep_count,
            'cache_count': cache_count,
            'error_count': error_count,
            'total_processed': current_idx + 1,
            'total_records': total_records
        }
        
        # Atualizar estatísticas a cada 1 registro para tempo real
        if (current_idx + 1) % 1 == 0:
            # Forçar atualização da interface através do st.rerun()
            time.sleep(0.01)  # Pequena pausa para permitir atualização
            
        # Rate limiting mais inteligente
        if status != "Cache":
            time.sleep(0.3)  # Reduzir delay para acelerar processamento
    
    # Finalizar processamento
    st.session_state['processing_state'] = PROCESSING_STATES['COMPLETED']
    st.session_state['processed_index'] = len(df)
    
    if progress_bar is not None:
        progress_bar.progress(1.0)
        if status_text is not None:
            status_text.text("Processamento concluído!")
    
    # Atualizar estatísticas finais
    st.session_state['current_stats'] = {
        'success_count': success_count,
        'cep_count': cep_count,
        'cache_count': cache_count,
        'error_count': error_count,
        'total_processed': len(df),
        'total_records': total_records
    }
    
    # Salvar cache final
    save_cache()
    log_capture.add_log('SUCCESS', "Processamento finalizado!", f"Total processado: {len(df)} registros")
    
    return df

# Configurar múltiplos geocoders
def get_geocoders():
    """Retorna lista de geocoders configurados com diferentes timeouts"""
    geocoders = []
    
    # Nominatim com timeout maior
    try:
        geocoders.append(('Nominatim', Nominatim(
            user_agent="ufes_geocoder_v3_robust"
        )))
    except Exception as e:
        logger.warning(f"Nominatim não disponível: {e}")
    
    # Photon (OpenStreetMap alternativo)
    try:
        geocoders.append(('Photon', Photon(
            user_agent="ufes_geocoder_v3_robust"
        )))
    except Exception as e:
        logger.warning(f"Photon não disponível: {e}")
    
    # ArcGIS (gratuito com limite)
    try:
        geocoders.append(('ArcGIS', ArcGIS()))
    except Exception as e:
        logger.warning(f"ArcGIS não disponível: {e}")
    
    return geocoders

# Funções de cache
def load_cache():
    """Carrega cache do disco"""
    global GEOCODING_CACHE
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'rb') as f:
                GEOCODING_CACHE = pickle.load(f)
            logger.info(f"Cache carregado com {len(GEOCODING_CACHE)} entradas")
    except Exception as e:
        logger.warning(f"Erro ao carregar cache: {e}")
        GEOCODING_CACHE = {}

def save_cache():
    """Salva cache no disco"""
    try:
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(GEOCODING_CACHE, f)
        logger.info(f"Cache salvo com {len(GEOCODING_CACHE)} entradas")
    except Exception as e:
        logger.warning(f"Erro ao salvar cache: {e}")

def get_cache_key(address: str) -> str:
    """Gera chave única para cache"""
    return hashlib.md5(address.lower().encode()).hexdigest()

# Função auxiliar para timeout customizado
def custom_geocode(geocoder, address, timeout=30):
    """Faz geocodificação com timeout customizado"""
    try:
        return geocoder.geocode(address, timeout=timeout)
    except Exception:
        # Se timeout como parâmetro falhar, tentar sem timeout
        return geocoder.geocode(address)

def geocode_with_retry(address: str, geocoders: list, max_retries: int = 3) -> Tuple[Optional[float], Optional[float], str, str]:
    """
    Geocodifica endereço com retry automático e múltiplos provedores
    
    Args:
        address: Endereço para geocodificar
        geocoders: Lista de geocoders
        max_retries: Número máximo de tentativas
    
    Returns:
        Tuple com (latitude, longitude, método, status)
    """
    if not address or pd.isna(address):
        return None, None, "Endereço vazio", "Falhou"
    
    # Verificar cache
    cache_key = get_cache_key(address)
    if cache_key in GEOCODING_CACHE:
        cached_result = GEOCODING_CACHE[cache_key]
        return cached_result[0], cached_result[1], cached_result[2], "Cache"
    
    # Tentar com cada geocoder
    for geocoder_name, geocoder in geocoders:
        for attempt in range(max_retries):
            try:
                logger.info(f"Tentativa {attempt + 1}/{max_retries} com {geocoder_name}: {address[:50]}...")
                
                # Fazer geocodificação com timeout customizado
                location = custom_geocode(geocoder, address, timeout=30)
                
                if location:
                    lat, lon = location.latitude, location.longitude
                    
                    # Validar coordenadas (Espírito Santo aproximadamente)
                    if -22.0 <= lat <= -17.0 and -42.0 <= lon <= -38.0:
                        # Salvar no cache
                        GEOCODING_CACHE[cache_key] = (lat, lon, geocoder_name, "Sucesso")
                        save_cache()
                        
                        logger.info(f"Sucesso com {geocoder_name}: {lat}, {lon}")
                        return lat, lon, geocoder_name, "Sucesso"
                    else:
                        logger.warning(f"Coordenadas fora do ES: {lat}, {lon}")
                        continue
                
                # Delay entre tentativas
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
                    
            except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable) as e:
                logger.warning(f"Erro com {geocoder_name}, tentativa {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
                continue
            except Exception as e:
                logger.error(f"Erro inesperado com {geocoder_name}: {e}")
                break
    
    # Falhou com todos os geocoders
    logger.warning(f"Falhou para: {address[:50]}...")
    return None, None, "Múltiplos provedores", "Falhou"

def geocode_address_robust(address: str, geocoders: list) -> Tuple[Optional[float], Optional[float], str, str]:
    """Geocodifica endereço com estratégia robusta"""
    return geocode_with_retry(address, geocoders)

def geocode_cep_robust(cep: str, geocoders: list) -> Tuple[Optional[float], Optional[float], str, str]:
    """Geocodifica CEP com estratégia robusta"""
    try:
        if pd.isna(cep) or str(cep).strip() == '':
            return None, None, "CEP", "Sem CEP"
        
        cep_clean = str(cep).replace('-', '').replace('.', '').strip()
        if len(cep_clean) != 8:
            return None, None, "CEP", "CEP inválido"
        
        cep_formatted = f"{cep_clean[:5]}-{cep_clean[5:]}"
        cep_address = f"{cep_formatted}, Espírito Santo, Brasil"
        
        lat, lon, method, status = geocode_with_retry(cep_address, geocoders)
        
        if lat is not None:
            return lat, lon, f"CEP-{method}", status
        return None, None, "CEP", "Falhou"
        
    except Exception as e:
        logger.error(f"Erro ao geocodificar CEP {cep}: {e}")
        return None, None, "CEP", "Erro"

# Função para geocodificação em batch
def geocode_batch(df: pd.DataFrame, geocoders: list, batch_size: int = 10) -> pd.DataFrame:
    """
    Geocodifica DataFrame em lotes para melhor performance
    
    Args:
        df: DataFrame com dados
        geocoders: Lista de geocoders
        batch_size: Tamanho do lote
    
    Returns:
        DataFrame com coordenadas
    """
    
    # Carregar cache
    load_cache()
    
    # Verificar se as colunas já existem antes de adicionar
    result_columns = ['latitude', 'longitude', 'geocoding_method', 'geocoding_status']
    
    # IMPORTANTE: Verificar se há duplicatas no DataFrame de entrada
    if df.columns.duplicated().any():
        st.warning("⚠️ Colunas duplicadas detectadas antes do processamento - corrigindo...")
        original_cols = df.columns.tolist()
        duplicated_cols = [col for col in original_cols if original_cols.count(col) > 1]
        
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        st.success(f"✅ Colunas duplicadas removidas antes do processamento: {list(set(duplicated_cols))}")
    
    for col in result_columns:
        if col not in df.columns:
            df[col] = None
        else:
            # Se a coluna já existe, limpar os valores
            df[col] = None
    
    # Progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Estatísticas
    stats_container = st.container()
    total_records = len(df)
    success_count = 0
    cep_count = 0
    error_count = 0
    cache_count = 0
    
    # Processar em lotes
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        
        for idx, row in batch.iterrows():
            # Atualizar progresso
            current_idx = i + (idx - batch.index[0])
            progress = (current_idx + 1) / total_records
            progress_bar.progress(progress)
            status_text.text(f"Processando {current_idx + 1}/{total_records} registros...")
            
            # Tentar geocodificar por endereço completo
            if pd.notna(row['endereco_completo']):
                lat, lon, method, status = geocode_address_robust(row['endereco_completo'], geocoders)
                
                if lat is not None:
                    df.loc[idx, 'latitude'] = lat
                    df.loc[idx, 'longitude'] = lon
                    df.loc[idx, 'geocoding_method'] = method
                    df.loc[idx, 'geocoding_status'] = status
                    
                    if status == "Cache":
                        cache_count += 1
                    else:
                        success_count += 1
                else:
                    # Tentar por CEP
                    cep_columns = [col for col in df.columns if 'cep' in col.lower() or col == 'V19']
                    if cep_columns:
                        lat, lon, method, status = geocode_cep_robust(row[cep_columns[0]], geocoders)
                        
                        if lat is not None:
                            df.loc[idx, 'latitude'] = lat
                            df.loc[idx, 'longitude'] = lon
                            df.loc[idx, 'geocoding_method'] = method
                            df.loc[idx, 'geocoding_status'] = status
                            
                            if status == "Cache":
                                cache_count += 1
                            else:
                                cep_count += 1
                        else:
                            df.loc[idx, 'geocoding_method'] = method
                            df.loc[idx, 'geocoding_status'] = status
                            error_count += 1
                    else:
                        df.loc[idx, 'geocoding_method'] = method
                        df.loc[idx, 'geocoding_status'] = status
                        error_count += 1
            else:
                # Tentar apenas por CEP
                cep_columns = [col for col in df.columns if 'cep' in col.lower() or col == 'V19']
                if cep_columns:
                    lat, lon, method, status = geocode_cep_robust(row[cep_columns[0]], geocoders)
                    
                    if lat is not None:
                        df.loc[idx, 'latitude'] = lat
                        df.loc[idx, 'longitude'] = lon
                        df.loc[idx, 'geocoding_method'] = method
                        df.loc[idx, 'geocoding_status'] = status
                        
                        if status == "Cache":
                            cache_count += 1
                        else:
                            cep_count += 1
                    else:
                        df.loc[idx, 'geocoding_method'] = method
                        df.loc[idx, 'geocoding_status'] = status
                        error_count += 1
                else:
                    df.loc[idx, 'geocoding_method'] = method
                    df.loc[idx, 'geocoding_status'] = status
                    error_count += 1
            
            # Atualizar estatísticas a cada 25 registros
            if (current_idx + 1) % 25 == 0:
                with stats_container:
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Sucesso", success_count)
                    with col2:
                        st.metric("CEP", cep_count)
                    with col3:
                        st.metric("Cache", cache_count)
                    with col4:
                        st.metric("Erro", error_count)
                    with col5:
                        st.metric("Taxa", f"{((success_count + cep_count + cache_count) / (current_idx + 1) * 100):.1f}%")
            
            # Rate limiting mais inteligente
            if status != "Cache":  # Não fazer delay para cache
                time.sleep(0.5)  # Delay menor
    
    # Finalizar progresso
    progress_bar.progress(1.0)
    status_text.text("Geocodificação concluída!")
    
    # Salvar cache final
    save_cache()
    
    return df

# Configuração da página
st.set_page_config(
    page_title="UFES Geocodificador v3.0",
    page_icon="●",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Moderno e Responsivo
def load_custom_css():
    st.markdown("""
    <style>
    /* Importação de fontes modernas */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* Reset e configurações globais */
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    
    /* Variáveis CSS */
    :root {
        --primary-color: #1a4480;
        --secondary-color: #4a6ba7;
        --accent-color: #6366f1;
        --success-color: #10b981;
        --warning-color: #f59e0b;
        --error-color: #ef4444;
        --background-primary: #ffffff;
        --background-secondary: #f8fafc;
        --background-card: #ffffff;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
        --border-color: #e2e8f0;
        --sidebar-bg: linear-gradient(180deg, #1a4480 0%, #4a6ba7 100%);
        --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
        --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        --shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
    }
    
    /* Background principal */
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
        padding: 1rem 0;
    }
    
    /* Container principal */
    .main .block-container {
        max-width: 1400px;
        padding: 1.5rem;
        background: var(--background-primary);
        border-radius: 20px;
        box-shadow: var(--shadow-xl);
        margin: 1rem auto;
        backdrop-filter: blur(10px);
    }
    
    /* Sidebar moderna */
    .css-1d391kg {
        background: var(--sidebar-bg);
        padding: 0;
    }
    
    .css-1d391kg .css-1544g2n {
        color: white;
        padding: 1rem;
    }
    
    .css-1d391kg .stMarkdown {
        color: white;
    }
    
    .css-1d391kg .stSelectbox > div > div > div {
        background: rgba(255, 255, 255, 0.1);
        border: 1px solid rgba(255, 255, 255, 0.2);
        border-radius: 8px;
        color: white;
    }
    
    .css-1d391kg .stSelectbox > div > div > div:hover {
        background: rgba(255, 255, 255, 0.2);
    }
    
    /* Menu de navegação na sidebar */
    .nav-menu {
        padding: 1rem 0;
        margin: 1rem 0;
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .nav-item {
        display: flex;
        align-items: center;
        padding: 12px 16px;
        margin: 8px 0;
        background: rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        color: white;
        text-decoration: none;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        cursor: pointer;
        font-weight: 500;
        font-size: 0.95rem;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .nav-item:hover {
        background: rgba(255, 255, 255, 0.2);
        transform: translateX(4px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    }
    
    .nav-item.active {
        background: rgba(255, 255, 255, 0.25);
        border-color: rgba(255, 255, 255, 0.3);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    }
    
    .nav-item .icon {
        margin-right: 12px;
        font-size: 1.2rem;
    }
    
    /* Header informativo compacto */
    .hero-section {
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%);
        color: white;
        padding: 2rem 2rem;
        border-radius: 16px;
        margin-bottom: 2rem;
        box-shadow: var(--shadow-md);
        position: relative;
        overflow: hidden;
        text-align: center;
    }
    
    .hero-section::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 100" fill="rgba(255,255,255,0.05)"><polygon points="0,0 1000,100 1000,0"/></svg>');
        background-size: cover;
        pointer-events: none;
    }
    

    
    .hero-title {
        font-size: 2.2rem;
        font-weight: 600;
        margin: 0 0 0.5rem 0;
        text-shadow: 0 1px 2px rgba(0,0,0,0.1);
        position: relative;
        z-index: 1;
    }
    
    .hero-subtitle {
        font-size: 1rem;
        margin: 0 0 1rem 0;
        opacity: 0.9;
        position: relative;
        z-index: 1;
        font-weight: 400;
    }
    
    .hero-description {
        font-size: 0.95rem;
        margin: 0;
        opacity: 0.8;
        position: relative;
        z-index: 1;
        max-width: 700px;
        margin: 0 auto;
        line-height: 1.5;
    }
    
    /* Feature cards */
    .feature-cards {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
        gap: 1.5rem;
        margin: 2rem 0;
    }
    
    .feature-card {
        background: var(--background-card);
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: var(--shadow-sm);
        border: 1px solid var(--border-color);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    
    .feature-card:hover {
        box-shadow: var(--shadow-md);
        transform: translateY(-2px);
    }
    
    .feature-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--accent-color), var(--primary-color));
    }
    
    .feature-icon {
        font-size: 2.5rem;
        margin-bottom: 0.75rem;
        color: var(--primary-color);
    }
    
    .feature-title {
        font-size: 1.3rem;
        font-weight: 600;
        margin-bottom: 0.75rem;
        color: var(--text-primary);
    }
    
    .feature-description {
        color: var(--text-secondary);
        line-height: 1.6;
        font-size: 0.95rem;
    }
    
    /* Cards modernos */
    .modern-card {
        background: var(--background-card);
        border-radius: 16px;
        padding: 2rem;
        margin: 1rem 0;
        box-shadow: var(--shadow-md);
        border: 1px solid var(--border-color);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    
    .modern-card:hover {
        box-shadow: var(--shadow-lg);
        transform: translateY(-2px);
    }
    
    .modern-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, var(--accent-color), var(--primary-color));
    }
    
    /* Botões modernos */
    .stButton > button {
        background: linear-gradient(135deg, var(--accent-color) 0%, var(--primary-color) 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 500;
        font-size: 1rem;
        box-shadow: var(--shadow-sm);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: var(--shadow-md);
    }
    
    .stButton > button:active {
        transform: translateY(0);
    }
    
    /* Métricas modernas */
    .css-1r6slb0 {
        background: var(--background-card);
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: var(--shadow-sm);
        border: 1px solid var(--border-color);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    .css-1r6slb0:hover {
        box-shadow: var(--shadow-md);
        transform: translateY(-2px);
    }
    
    /* Progress bar moderna */
    .stProgress > div > div {
        background: linear-gradient(90deg, var(--accent-color), var(--primary-color));
        border-radius: 10px;
        height: 12px;
    }
    
    .stProgress > div {
        background: var(--background-secondary);
        border-radius: 10px;
        height: 12px;
    }
    
    /* Inputs modernos */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stSelectbox > div > div > select {
        border-radius: 12px;
        border: 2px solid var(--border-color);
        padding: 12px 16px;
        font-size: 1rem;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        background: var(--background-card);
    }
    
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus,
    .stSelectbox > div > div > select:focus {
        border-color: var(--accent-color);
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
        outline: none;
    }
    
    /* File uploader moderno */
    .stFileUploader > div {
        background: var(--background-card);
        border: 2px dashed var(--border-color);
        border-radius: 16px;
        padding: 2rem;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    .stFileUploader > div:hover {
        border-color: var(--accent-color);
        background: rgba(99, 102, 241, 0.02);
    }
    
    /* Dataframe moderno */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: var(--shadow-sm);
        border: 1px solid var(--border-color);
    }
    
    /* Alerts modernos */
    .stAlert {
        border-radius: 12px;
        padding: 1rem 1.5rem;
        border: none;
        box-shadow: var(--shadow-sm);
    }
    
    /* Footer moderno */
    .footer {
        background: var(--background-card);
        border-radius: 16px;
        padding: 2rem;
        margin-top: 3rem;
        box-shadow: var(--shadow-sm);
        border: 1px solid var(--border-color);
        text-align: center;
    }
    

    
    .footer-text {
        color: var(--text-secondary);
        font-size: 0.9rem;
        line-height: 1.5;
        margin: 0.5rem 0;
    }
    
    .footer-brand {
        color: var(--primary-color);
        font-weight: 600;
    }
    
    /* Animações */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes slideIn {
        from { transform: translateX(-20px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    
    .fade-in {
        animation: fadeIn 0.6s ease-out;
    }
    
    .slide-in {
        animation: slideIn 0.6s ease-out;
    }
    
    /* Responsividade */
    @media (max-width: 768px) {
        .main .block-container {
            padding: 1rem;
            margin: 0.5rem;
        }
        
        .hero-section {
            padding: 1.5rem 1rem;
        }
        
        .hero-title {
            font-size: 1.8rem;
        }
        
        .hero-subtitle {
            font-size: 0.9rem;
        }
        

        
        .feature-cards {
            grid-template-columns: 1fr;
            gap: 1rem;
            margin: 1.5rem 0;
        }
        
        .modern-card {
            padding: 1.25rem;
        }
        
        .nav-item {
            padding: 10px 12px;
            font-size: 0.9rem;
        }
        
        .hero-description {
            font-size: 0.9rem;
        }
        
        .feature-card {
            padding: 1.25rem;
        }
    }
    
    @media (max-width: 480px) {
        .hero-title {
            font-size: 1.6rem;
        }
        
        .hero-subtitle {
            font-size: 0.85rem;
        }
        

        
        .feature-icon {
            font-size: 2rem;
        }
        
        .feature-title {
            font-size: 1.1rem;
        }
        
        .hero-section {
            padding: 1.25rem 1rem;
        }
        
        .feature-card {
            padding: 1rem;
        }
    }
    
    /* Scrollbar personalizada */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: var(--background-secondary);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: var(--border-color);
        border-radius: 10px;
        transition: all 0.3s ease;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: var(--text-secondary);
    }
    </style>
    """, unsafe_allow_html=True)

# Função para carregar logo
def get_logo_base64():
    """Carrega e converte a logo para base64"""
    # Obter o diretório do script atual
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Tentar diferentes caminhos para a logo
    logo_paths = [
        os.path.join(script_dir, "Logo", "Logo.png"),
        os.path.join(script_dir, "logo", "Logo.png"),
        os.path.join(script_dir, "Logo", "logo.png"),
        os.path.join(script_dir, "logo", "logo.png"),
        "Logo/Logo.png",
        "logo/Logo.png", 
        "Logo/logo.png",
        "logo/logo.png"
    ]
    
    for logo_path in logo_paths:
        if os.path.exists(logo_path):
            try:
                with open(logo_path, "rb") as f:
                    data = base64.b64encode(f.read()).decode()
                    print(f"✅ Logo carregada: {logo_path} ({len(data)} caracteres)")
                    return data
            except Exception as e:
                print(f"❌ Erro ao carregar logo: {e}")
                continue
    print("❌ Nenhuma logo encontrada")
    return None

# Função para criar homepage
def create_homepage():
    """Cria a página inicial"""
    # Feature cards usando colunas do Streamlit
    st.markdown("### Funcionalidades Principais")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="modern-card">
            <h4 style="color: var(--primary-color); margin-bottom: 0.75rem; text-align: center;">Processamento CNPJ</h4>
            <p style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.6; text-align: center;">
                Carregue arquivos Excel com dados CNPJ e processe automaticamente com construção 
                inteligente de endereços e geocodificação em lote.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="modern-card">
            <h4 style="color: var(--primary-color); margin-bottom: 0.75rem; text-align: center;">Visualização Avançada</h4>
            <p style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.6; text-align: center;">
                Visualize dados já processados em mapas interativos com suporte a múltiplos 
                formatos (CSV, GeoJSON, Excel).
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="modern-card">
            <h4 style="color: var(--primary-color); margin-bottom: 0.75rem; text-align: center;">Analytics & Exports</h4>
            <p style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.6; text-align: center;">
                Relatórios detalhados de geocodificação com exportação em CSV, GeoJSON e 
                Shapefile para uso em sistemas GIS.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    # Informações técnicas
    st.markdown("""
    <div class="modern-card">
        <h3>Especificações Técnicas</h3>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; margin-top: 1rem;">
            <div>
                <h4 style="color: var(--primary-color); margin-bottom: 0.5rem;">Precisão</h4>
                <p style="margin: 0;">Taxa de geocodificação: 80-90%</p>
            </div>
            <div>
                <h4 style="color: var(--primary-color); margin-bottom: 0.5rem;">Performance</h4>
                <p style="margin: 0;">Processamento: 1 req/segundo</p>
            </div>
            <div>
                <h4 style="color: var(--primary-color); margin-bottom: 0.5rem;">Cobertura</h4>
                <p style="margin: 0;">Foco: Espírito Santo, Brasil</p>
            </div>
            <div>
                <h4 style="color: var(--primary-color); margin-bottom: 0.5rem;">Formatos</h4>
                <p style="margin: 0;">Excel, CSV, GeoJSON, Shapefile</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# Funcionalidades principais (mantidas do código original)
def clean_data(df):
    """Limpa e prepara os dados CNPJ"""
    df_clean = df.copy()
    
    # Primeiro, limpar nomes das colunas (remover aspas e espaços)
    clean_columns = [col.strip().strip('"').strip("'") for col in df_clean.columns]
    df_clean.columns = clean_columns
    
    # Limpar aspas das colunas de texto (CNPJ padrão)
    text_columns = ['V14', 'V15', 'V16', 'V18', 'V19']
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip('"').str.strip("'")
    
    # Também limpar qualquer coluna que contenha "cep" no nome
    cep_columns = [col for col in df_clean.columns if 'cep' in col.lower()]
    for col in cep_columns:
        df_clean[col] = df_clean[col].astype(str).str.strip('"').str.strip("'")
    
    # Limpar valores NaN
    df_clean = df_clean.fillna('')
    
    return df_clean

def construct_address(row):
    """Constrói endereço completo a partir das colunas"""
    try:
        # Se já existe endereco_completo, usar ele
        if 'endereco_completo' in row.index and pd.notna(row.get('endereco_completo', '')) and row.get('endereco_completo', '').strip():
            return row['endereco_completo']
        
        parts = []
        
        # Tipo de logradouro + logradouro (apenas se existirem as colunas)
        if pd.notna(row.get('V14', '')) and row.get('V14', '').strip():
            parts.append(str(row['V14']).strip())
        
        if pd.notna(row.get('V15', '')) and row.get('V15', '').strip():
            parts.append(str(row['V15']).strip())
        
        # Número (apenas se válido)
        numero = str(row.get('V16', '')).strip().upper()
        if numero and numero not in ['S/N', 'SN', 'NAN', '']:
            parts.append(numero)
        
        # Complemento/Bairro
        if pd.notna(row.get('V18', '')) and row.get('V18', '').strip():
            parts.append(str(row['V18']).strip())
        
        # Montar endereço
        if parts:
            endereco = ', '.join(parts)
            endereco += ', Espírito Santo, Brasil'
            return endereco
        
        return None
    except Exception as e:
        return None

def geocode_address(address, geocoder):
    """Geocodifica um endereço"""
    try:
        location = geocoder.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude, "Endereço completo", "Sucesso"
        return None, None, "Endereço completo", "Falhou"
    except (GeocoderTimedOut, GeocoderServiceError):
        return None, None, "Endereço completo", "Erro"

def geocode_cep(cep, geocoder):
    """Geocodifica usando CEP"""
    try:
        if pd.isna(cep) or str(cep).strip() == '':
            return None, None, "CEP", "Sem CEP"
        
        cep_clean = str(cep).replace('-', '').replace('.', '').strip()
        if len(cep_clean) != 8:
            return None, None, "CEP", "CEP inválido"
        
        cep_formatted = f"{cep_clean[:5]}-{cep_clean[5:]}"
        location = geocoder.geocode(f"{cep_formatted}, Espírito Santo, Brasil", timeout=10)
        
        if location:
            return location.latitude, location.longitude, "CEP", "Sucesso"
        return None, None, "CEP", "Falhou"
    except (GeocoderTimedOut, GeocoderServiceError):
        return None, None, "CEP", "Erro"

def create_folium_map(df_geo):
    """Cria mapa Folium moderno e interativo"""
    
    # Fazer cópia e limpar duplicatas antes de processar
    df_clean = df_geo.copy()
    if df_clean.columns.duplicated().any():
        df_clean = df_clean.loc[:, ~df_clean.columns.duplicated(keep='first')]
        print(f"✅ Colunas duplicadas removidas no mapa")
    
    # Centro do Espírito Santo
    center_lat, center_lon = -20.3155, -40.3128
    
    # Criar mapa base com estilo mais claro
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=8,
        tiles='CartoDB positron'  # Começar com mapa claro
    )
    
    # Adicionar tiles alternativos
    folium.TileLayer('OpenStreetMap', name='OpenStreetMap').add_to(m)
    folium.TileLayer('CartoDB dark_matter', name='CartoDB Dark').add_to(m)
    folium.TileLayer('CartoDB voyager', name='CartoDB Voyager').add_to(m)
    
    # Cores mais vibrantes e contrastantes
    colors = {
        'Endereço completo': '#22c55e',  # Verde vibrante
        'CEP': '#f97316',               # Laranja vibrante
        'Falhou': '#ef4444',            # Vermelho
        'Erro': '#6b7280'               # Cinza
    }
    
    # Dicionário de mapeamento dos campos CNPJ
    field_mapping = {
        'V12': 'Código do Município',
        'V14': 'Tipo de Logradouro',
        'V15': 'Nome do Logradouro',
        'V16': 'Número',
        'V18': 'Bairro/Complemento',
        'V19': 'CEP',
        'V21': 'Código do Bairro',
        'anos_atividade': 'Anos de Atividade',
        'CNAE_referencia': 'CNAE Referência',
        'id': 'ID do Registro'
    }
    
    # Adicionar marcadores mais visíveis e interativos
    for idx, row in df_clean.iterrows():
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
            method = row.get('geocoding_method', 'Desconhecido')
            color = colors.get(method, '#6b7280')
            
            # Buscar CNPJ ou ID para o cabeçalho
            cnpj_value = 'N/A'
            header_id = f"Estabelecimento #{row.get('id', idx + 1)}"
            
            # Verificar se existe coluna CNPJ
            cnpj_columns = [col for col in df_clean.columns if 'cnpj' in col.lower()]
            if cnpj_columns:
                cnpj_value = row.get(cnpj_columns[0], 'N/A')
                if cnpj_value != 'N/A' and str(cnpj_value).strip():
                    header_id = f"CNPJ: {cnpj_value}"
            
            # Construir informações dos campos
            field_info = ""
            for field, description in field_mapping.items():
                if field in df_clean.columns:
                    value = row.get(field, 'N/A')
                    if pd.notna(value) and str(value).strip() and str(value) != 'N/A':
                        # Limpar valor
                        clean_value = str(value).strip().strip('"').strip("'")
                        field_info += f"""
                        <div style="margin: 4px 0; padding: 3px 0; border-bottom: 1px solid #e2e8f0;">
                            <strong style="color: #2d3748; font-size: 11px;">{field} - {description}:</strong><br>
                            <span style="color: #4a5568; font-size: 12px;">{clean_value}</span>
                        </div>
                        """
            
            # Popup mais bonito e informativo
            popup_html = f"""
            <div style="font-family: 'Inter', sans-serif; min-width: 280px; max-width: 380px;">
                <div style="background: {color}; color: white; padding: 10px; margin: -10px -10px 12px -10px; border-radius: 6px 6px 0 0;">
                    <h4 style="margin: 0; font-size: 13px; font-weight: 600;">{header_id}</h4>
                    <p style="margin: 3px 0 0 0; font-size: 11px; opacity: 0.9;">Status: {method}</p>
                </div>
                <div style="padding: 5px 0; max-height: 300px; overflow-y: auto;">
                    {field_info}
                    <div style="margin-top: 10px; padding-top: 8px; border-top: 2px solid #e2e8f0;">
                        <p style="margin: 3px 0;"><strong style="color: #2d3748;">📍 Endereço Completo:</strong><br>
                        <span style="color: #4a5568; font-size: 11px;">{row.get('endereco_completo', 'N/A')}</span></p>
                        <p style="margin: 3px 0;"><strong style="color: #2d3748;">🌍 Coordenadas:</strong><br>
                        <span style="color: #4a5568; font-family: monospace; font-size: 11px;">{row['latitude']:.6f}, {row['longitude']:.6f}</span></p>
                    </div>
                </div>
            </div>
            """
            
            # Escolher ícone baseado no método
            if method == 'Endereço completo':
                icon_symbol = '✓'
                icon_color = 'green'
            elif method == 'CEP':
                icon_symbol = '📮'
                icon_color = 'orange'
            else:
                icon_symbol = '⚠'
                icon_color = 'red'
            
            # Marcador mais visível
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_html, max_width=450),
                tooltip=f"{header_id} - {method}",
                icon=folium.Icon(
                    color=icon_color,
                    icon='map-pin',
                    prefix='fa'
                )
            ).add_to(m)
    
    # Adicionar controle de layers
    folium.LayerControl().add_to(m)
    
    return m

def create_downloads(df_geo):
    """Cria arquivos para download"""
    downloads = {}
    
    # Fazer uma cópia profunda para evitar modificar o original
    df_clean = df_geo.copy()
    
    # Tratamento robusto de colunas duplicadas
    if df_clean.columns.duplicated().any():
        duplicated_cols = df_clean.columns[df_clean.columns.duplicated()].tolist()
        
        # Método mais robusto: recriar DataFrame com colunas únicas
        unique_columns = []
        seen_columns = set()
        
        for col in df_clean.columns:
            if col not in seen_columns:
                unique_columns.append(col)
                seen_columns.add(col)
        
        # Recriar DataFrame mantendo apenas colunas únicas
        df_clean = df_clean.loc[:, ~df_clean.columns.duplicated(keep='first')]
        
        print(f"✅ Colunas duplicadas removidas nos downloads: {list(set(duplicated_cols))}")
        st.info(f"✅ Colunas duplicadas removidas nos downloads: {list(set(duplicated_cols))}")
    
    # Verificar se as colunas essenciais existem
    if 'latitude' not in df_clean.columns or 'longitude' not in df_clean.columns:
        st.error("❌ Colunas latitude ou longitude não encontradas no DataFrame")
        return downloads
    
    # CSV
    csv_buffer = df_clean.to_csv(index=False)
    downloads['CSV'] = csv_buffer
    
    # GeoJSON
    if not df_clean.empty:
        # Criar GeoDataFrame sem duplicar colunas de coordenadas
        valid_rows = df_clean.dropna(subset=['latitude', 'longitude'])
        if not valid_rows.empty:
            try:
                geometry = [Point(xy) for xy in zip(valid_rows['longitude'], valid_rows['latitude'])]
                if geometry:
                    # Criar cópia do DataFrame sem conflitos
                    df_for_geo = valid_rows.copy()
                    # Verificar novamente se não há duplicatas antes de criar GeoDataFrame
                    if df_for_geo.columns.duplicated().any():
                        df_for_geo = df_for_geo.loc[:, ~df_for_geo.columns.duplicated(keep='first')]
                    
                    # Criar GeoDataFrame e depois remover colunas latitude/longitude duplicadas
                    geo_df = gpd.GeoDataFrame(df_for_geo, geometry=geometry, crs='EPSG:4326')
                    
                    # IMPORTANTE: Remover colunas de coordenadas porque a geometria já as contém
                    # Isso evita duplicação no GeoJSON
                    if 'latitude' in geo_df.columns:
                        geo_df = geo_df.drop('latitude', axis=1)
                    if 'longitude' in geo_df.columns:
                        geo_df = geo_df.drop('longitude', axis=1)
                    
                    geojson_str = geo_df.to_json()
                    downloads['GeoJSON'] = geojson_str
            except Exception as e:
                st.error(f"Erro ao criar GeoJSON: {e}")
    
    # Shapefile
    if not df_clean.empty and 'valid_rows' in locals() and not valid_rows.empty:
        try:
            # Usar o mesmo geo_df criado para GeoJSON
            if 'geo_df' in locals():
                with tempfile.TemporaryDirectory() as temp_dir:
                    shp_path = os.path.join(temp_dir, 'geocoded_data.shp')
                    geo_df.to_file(shp_path)
                    
                    # Criar ZIP com todos os arquivos do shapefile
                    zip_path = os.path.join(temp_dir, 'shapefile.zip')
                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        for ext in ['.shp', '.shx', '.dbf', '.prj']:
                            file_path = shp_path.replace('.shp', ext)
                            if os.path.exists(file_path):
                                zipf.write(file_path, f'geocoded_data{ext}')
                    
                    # Ler o arquivo ZIP e fechar o handle
                    with open(zip_path, 'rb') as f:
                        zip_data = f.read()
                    downloads['Shapefile'] = zip_data
        except Exception as e:
            st.error(f"Erro ao criar Shapefile: {e}")
    
    return downloads

# Página de processamento CNPJ
# Melhorar o cache para evitar reprocessamento
@st.cache_data(ttl=300)  # Cache por 5 minutos
def process_uploaded_files(uploaded_files_data):
    """Processa arquivos com cache para evitar reprocessamento"""
    dfs = []
    for file_content, file_name in uploaded_files_data:
        df = pd.read_excel(BytesIO(file_content))
        df_clean = clean_data(df)
        dfs.append(df_clean)
    
    # Combinar DataFrames
    df_combined = pd.concat(dfs, ignore_index=True)
    
    # Construir endereços
    df_combined['endereco_completo'] = df_combined.apply(construct_address, axis=1)
    
    return df_combined

def show_cnpj_processing():
    """Mostra a página de processamento CNPJ"""
    st.markdown('<div class="fade-in">', unsafe_allow_html=True)
    
    st.markdown("## Processamento de Dados CNPJ")
    st.markdown("Carregue arquivos Excel com dados CNPJ para geocodificação automatizada.")
    
    # Upload de arquivos
    st.markdown("### Upload de Arquivos")
    uploaded_files = st.file_uploader(
        "Selecione os arquivos Excel com dados CNPJ",
        type=['xlsx', 'xls'],
        accept_multiple_files=True,
        help="Faça upload dos arquivos CNPJs_ES_P1_V2.xlsx e CNPJs_ES_P2_V2.xlsx"
    )
    
    if uploaded_files:
        # Verificar se os arquivos mudaram para evitar reprocessamento
        file_key = "_".join([f.name for f in uploaded_files])
        
        if 'last_file_key' not in st.session_state or st.session_state['last_file_key'] != file_key:
            with st.spinner("Processando arquivos..."):
                # Preparar dados para cache
                files_data = [(file.read(), file.name) for file in uploaded_files]
                
                # Usar função com cache
                df_combined = process_uploaded_files(files_data)
                
                # Salvar no session state
                st.session_state['df_processed'] = df_combined
                st.session_state['last_file_key'] = file_key
                
        else:
            # Usar dados já processados
            df_combined = st.session_state['df_processed']
        
        st.success("Arquivos processados com sucesso!")
        
        # Mostrar informações sobre as colunas disponíveis
        st.markdown("### Informações do Arquivo")
        st.info(f"Colunas disponíveis: {', '.join(df_combined.columns.tolist())}")
        
        # Verificar se tem a estrutura esperada
        expected_columns = ['V14', 'V15', 'V16', 'V18', 'V19']
        missing_columns = [col for col in expected_columns if col not in df_combined.columns]
        
        if missing_columns:
            st.warning(f"Colunas esperadas não encontradas: {', '.join(missing_columns)}")
            st.markdown("""
            **Como proceder:**
            1. Verifique se o arquivo tem a estrutura CNPJ padrão
            2. Certifique-se de que as colunas estão nomeadas corretamente
            3. Para arquivos com estrutura diferente, o sistema tentará identificar automaticamente colunas de CEP
            """)
        else:
            st.success("Arquivo com estrutura CNPJ padrão identificado!")
        
        # Diagnóstico específico para arquivos CNPJ originais
        st.markdown("### Diagnóstico de Compatibilidade")
        
        if all(col in df_combined.columns for col in ['V14', 'V15', 'V16', 'V18', 'V19']):
            st.success("Estrutura CNPJ padrão detectada! Arquivos compatíveis com CNPJs_ES_P1_V2.xlsx e CNPJs_ES_P2_V2.xlsx")
            
            # Verificar qualidade dos dados
            col1, col2 = st.columns(2)
            with col1:
                v14_count = df_combined['V14'].notna().sum() if 'V14' in df_combined.columns else 0
                v15_count = df_combined['V15'].notna().sum() if 'V15' in df_combined.columns else 0
                st.info(f"**Dados de Endereço:**\n- Tipo logradouro (V14): {v14_count:,} registros\n- Nome logradouro (V15): {v15_count:,} registros")
            
            with col2:
                # Buscar coluna CEP dinamicamente
                cep_columns = [col for col in df_combined.columns if 'cep' in col.lower() or col == 'V19']
                v19_count = df_combined[cep_columns[0]].notna().sum() if cep_columns else 0
                v18_count = df_combined['V18'].notna().sum() if 'V18' in df_combined.columns else 0
                cep_nome = cep_columns[0] if cep_columns else "V19"
                st.info(f"**Dados Complementares:**\n- CEP ({cep_nome}): {v19_count:,} registros\n- Bairro (V18): {v18_count:,} registros")
        
        # Mostrar mapeamento de colunas esperado
        with st.expander("Ver Mapeamento de Colunas Esperado"):
            st.markdown("""
            **Estrutura esperada para arquivos CNPJ:**
            - `V14`: Tipo de logradouro (RUA, AVENIDA, etc.)
            - `V15`: Nome do logradouro
            - `V16`: Número do endereço
            - `V18`: Complemento/Bairro
            - `V19`: CEP
            - `V12`: Código do município
            
            **Arquivos testados:**
            - ✅ CNPJs_ES_P1_V2.xlsx
            - ✅ CNPJs_ES_P2_V2.xlsx
            - ✅ dicionario_estabelecimentos_transposto_CNPJ_Modificado.xlsx
            
            **Observação:** O sistema também identifica automaticamente colunas que contenham "cep" no nome.
            """)
        
        # Estatísticas detalhadas
        st.markdown("### Estatísticas do Arquivo")
        
        total_registros = len(df_combined)
        endereco_count = df_combined['endereco_completo'].notna().sum() if 'endereco_completo' in df_combined.columns else 0
        
        # Verificar se existe coluna CEP (V19 ou outras variações)
        cep_columns = [col for col in df_combined.columns if 'cep' in col.lower() or col == 'V19']
        if cep_columns:
            cep_count = df_combined[cep_columns[0]].notna().sum()
            cep_info = f"{cep_count:,}"
            cep_coluna = cep_columns[0]
        else:
            cep_count = 0
            cep_info = "N/A"
            cep_coluna = "Não encontrada"
        
        # Exibir informações principais
        st.info(f"""
        **Resumo da Leitura:**
        - **Total de registros lidos:** {total_registros:,} registros
        - **Tamanho do arquivo:** ~{total_registros/1000:.1f}k registros
        - **Colunas identificadas:** {len(df_combined.columns)} colunas
        - **Coluna CEP identificada:** {cep_coluna}
        """)
        
        # Métricas em colunas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de Registros", f"{total_registros:,}")
        with col2:
            st.metric("Com Endereço", f"{endereco_count:,}")
        with col3:
            st.metric("Com CEP", cep_info)
        with col4:
            if endereco_count > 0:
                taxa = (endereco_count / total_registros * 100)
                st.metric("Taxa de Completude", f"{taxa:.1f}%")
            else:
                st.metric("Taxa de Completude", "0%")
        
        # Prévia dos dados
        st.markdown("### Prévia dos Dados")
        st.dataframe(df_combined.head(10), use_container_width=True)
        
        # Configurações de geocodificação
        st.markdown("### Configurações de Geocodificação")
        
        col1, col2 = st.columns(2)
        with col1:
            mode = st.selectbox(
                "Modo de Processamento",
                ["Teste (Amostra)", "Completo (Todos os registros)"],
                help="Teste processa uma amostra para validação rápida"
            )
        
        with col2:
            if mode == "Teste (Amostra)":
                sample_size = st.slider("Tamanho da Amostra", 10, 1000, 100)
            else:
                sample_size = len(df_combined)
        
        # Preparar dados para processamento
        df_to_process = df_combined.copy()
        if mode == "Teste (Amostra)":
            df_to_process = df_to_process.sample(n=min(sample_size, len(df_to_process))).reset_index(drop=True)
        
        # Salvar dados no session state
        st.session_state['df_to_process'] = df_to_process
        
        # Inicializar geocoders
        geocoders = get_geocoders()
        
        if not geocoders:
            st.error("❌ Nenhum geocoder disponível. Verifique sua conexão com a internet.")
            return
            
        st.info(f"🔧 Usando {len(geocoders)} geocoders: {', '.join([name for name, _ in geocoders])}")
        st.session_state['geocoders'] = geocoders
        
        # Área de controle do processamento
        st.markdown("### 🎮 Controle de Processamento")
        
        # Botões de controle
        start_clicked, pause_clicked, stop_clicked, download_clicked = create_control_buttons()
        
        # Ações dos botões
        if start_clicked:
            st.session_state['processing_state'] = PROCESSING_STATES['RUNNING']
            st.session_state['processed_index'] = 0
            log_capture.clear_logs()
            log_capture.add_log('INFO', "Iniciando processamento...")
            st.rerun()
        
        if pause_clicked:
            current_state = st.session_state.get('processing_state', PROCESSING_STATES['IDLE'])
            if current_state == PROCESSING_STATES['RUNNING']:
                st.session_state['processing_state'] = PROCESSING_STATES['PAUSED']
                log_capture.add_log('INFO', "Processamento pausado pelo usuário")
            elif current_state == PROCESSING_STATES['PAUSED']:
                st.session_state['processing_state'] = PROCESSING_STATES['RUNNING']
                log_capture.add_log('INFO', "Processamento retomado pelo usuário")
            st.rerun()
        
        if stop_clicked:
            st.session_state['processing_state'] = PROCESSING_STATES['STOPPED']
            log_capture.add_log('WARNING', "Processamento interrompido pelo usuário")
            st.rerun()
        
        if download_clicked and 'partial_results' in st.session_state:
            partial_data = st.session_state['partial_results']
            st.info(f"📥 Preparando download dos dados parciais ({partial_data['processed_count']} registros processados)")
            
            # Criar downloads dos dados parciais
            partial_df = partial_data['df']
            downloads = create_downloads(partial_df)
            
            st.markdown("### 📥 Downloads Parciais")
            for download_type, download_data in downloads.items():
                st.download_button(
                    label=f"📥 {download_type}",
                    data=download_data['data'],
                    file_name=f"parcial_{download_data['filename']}",
                    mime=download_data['mime'],
                    use_container_width=True
                )
        
        # Exibir status atual
        current_state = st.session_state.get('processing_state', PROCESSING_STATES['IDLE'])
        processed_count = st.session_state.get('processed_index', 0)
        
        if current_state != PROCESSING_STATES['IDLE']:
            st.markdown("### 📊 Status do Processamento")
            
            # Criar containers globais para atualizações em tempo real
            if 'progress_container' not in st.session_state:
                st.session_state['progress_container'] = st.empty()
            if 'stats_container' not in st.session_state:
                st.session_state['stats_container'] = st.empty()
            if 'log_container' not in st.session_state:
                st.session_state['log_container'] = st.empty()
            
            # Status básico SEMPRE visível
            status_col1, status_col2, status_col3 = st.columns(3)
            with status_col1:
                status_emoji = {
                    PROCESSING_STATES['RUNNING']: '🟢',
                    PROCESSING_STATES['PAUSED']: '🟡',
                    PROCESSING_STATES['STOPPED']: '🔴',
                    PROCESSING_STATES['COMPLETED']: '✅'
                }
                st.metric("Status", f"{status_emoji.get(current_state, '⚪')} {current_state.upper()}")
            
            with status_col2:
                # Mostrar progresso em tempo real
                if 'current_stats' in st.session_state:
                    stats = st.session_state['current_stats']
                    st.metric("Processados", f"{stats['total_processed']}/{stats['total_records']}")
                else:
                    st.metric("Processados", f"{processed_count}/{len(df_to_process)}")
            
            with status_col3:
                # Mostrar percentual em tempo real
                if 'current_stats' in st.session_state:
                    stats = st.session_state['current_stats']
                    progress_percent = (stats['total_processed'] / stats['total_records']) * 100 if stats['total_records'] > 0 else 0
                    st.metric("Progresso", f"{progress_percent:.1f}%")
                else:
                    progress_percent = (processed_count / len(df_to_process)) * 100 if len(df_to_process) > 0 else 0
                    st.metric("Progresso", f"{progress_percent:.1f}%")
            
            # Separador visual
            st.divider()
            
            # Métricas detalhadas removidas conforme solicitado
                
            # Progresso e logs containers
            st.markdown("#### 🔄 Progresso")
            with st.session_state['progress_container']:
                if current_state == PROCESSING_STATES['RUNNING']:
                    st.info("Processamento em andamento...")
                    
            st.markdown("#### 📝 Logs do Processamento")
            with st.session_state['log_container']:
                if current_state != PROCESSING_STATES['IDLE']:
                    display_logs("main_interface")
        
        # Iniciar processamento se estado for RUNNING
        if current_state == PROCESSING_STATES['RUNNING'] and 'df_to_process' in st.session_state and 'geocoders' in st.session_state:
            try:
                # Atualizar interface em tempo real durante processamento
                with st.spinner("Processando geocodificação..."):
                    df_geocoded = geocode_batch_with_control(
                        st.session_state['df_to_process'], 
                        st.session_state['geocoders'], 
                        batch_size=10
                    )
                
                # Salvar resultados
                st.session_state['df_geocoded'] = df_geocoded
                
                # Atualizar métricas finais
                if 'current_stats' in st.session_state:
                    stats = st.session_state['current_stats']
                    st.session_state['stats_container'].empty()
                    with st.session_state['stats_container'].container():
                        col1, col2, col3, col4, col5 = st.columns(5)
                        with col1:
                            st.metric("✅ Sucesso", stats['success_count'])
                        with col2:
                            st.metric("📮 CEP", stats['cep_count'])
                        with col3:
                            st.metric("💾 Cache", stats['cache_count'])
                        with col4:
                            st.metric("❌ Erro", stats['error_count'])
                        with col5:
                            if stats['total_processed'] > 0:
                                taxa = ((stats['success_count'] + stats['cep_count'] + stats['cache_count']) / stats['total_processed'] * 100)
                                st.metric("🎯 Taxa", f"{taxa:.1f}%")
                            else:
                                st.metric("🎯 Taxa", "0%")
                
                # Estatísticas finais removidas conforme solicitado
                st.success("✅ Geocodificação concluída com sucesso!")
                
            except Exception as e:
                st.error(f"❌ Erro durante a geocodificação: {str(e)}")
                log_capture.add_log('ERROR', f"Erro fatal", f"Erro: {str(e)}")
                logger.error(f"Erro na geocodificação: {e}")
                return
        
        # Logs removidos desta posição - agora ficam no final da página

    
    # Visualização e downloads
    if 'df_geocoded' in st.session_state:
        df_geo = st.session_state['df_geocoded']
        
        # Mapa
        st.markdown("### Visualização no Mapa")
        df_mapped = df_geo.dropna(subset=['latitude', 'longitude'])
        
        if not df_mapped.empty:
            folium_map = create_folium_map(df_mapped)
            folium_static(folium_map, width=1400, height=600)
        else:
            st.warning("Nenhum registro foi geocodificado com sucesso.")
        
        # Downloads
        st.markdown("### Downloads")
        downloads = create_downloads(df_geo)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'CSV' in downloads:
                st.download_button(
                    label="Download CSV",
                    data=downloads['CSV'],
                    file_name=f"geocoded_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        with col2:
            if 'GeoJSON' in downloads:
                st.download_button(
                    label="Download GeoJSON",
                    data=downloads['GeoJSON'],
                    file_name=f"geocoded_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.geojson",
                    mime="application/json",
                    use_container_width=True
                )
        
        with col3:
            if 'Shapefile' in downloads:
                st.download_button(
                    label="Download Shapefile",
                    data=downloads['Shapefile'],
                    file_name=f"geocoded_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.zip",
                    mime="application/zip",
                    use_container_width=True
                )
    
    # Seção de logs no final da página (antes do rodapé)
    current_state = st.session_state.get('processing_state', PROCESSING_STATES['IDLE'])
    if current_state != PROCESSING_STATES['IDLE']:
        st.markdown("---")
        display_logs("main_interface")
    
    st.markdown('</div>', unsafe_allow_html=True)

# Função para processar arquivos de visualização com cache
def remove_duplicate_columns_robust(df, file_name="arquivo"):
    """Função robusta para remover colunas duplicadas de qualquer DataFrame"""
    if df is None:
        return None
        
    original_shape = df.shape
    original_columns = df.columns.tolist()
    
    # Primeira passada: remover duplicatas óbvias
    if df.columns.duplicated().any():
        st.warning(f"⚠️ Colunas duplicadas detectadas em {file_name}")
        
        # Encontrar todas as colunas duplicadas
        duplicated_mask = df.columns.duplicated(keep='first')
        duplicated_cols = df.columns[duplicated_mask].tolist()
        
        # Remover duplicatas mantendo apenas a primeira ocorrência
        df = df.loc[:, ~duplicated_mask]
        
        st.success(f"✅ Removidas {len(duplicated_cols)} colunas duplicadas: {list(set(duplicated_cols))}")
    
    # Segunda passada: verificar duplicatas case-insensitive
    cols_lower = {}
    for col in df.columns:
        col_key = col.lower().strip()
        if col_key in cols_lower:
            # Encontrou duplicata case-insensitive
            st.warning(f"⚠️ Coluna com nome similar encontrada: '{col}' vs '{cols_lower[col_key]}'")
            # Remover a segunda ocorrência
            df = df.drop(columns=[col], errors='ignore')
            st.success(f"✅ Coluna '{col}' removida (similar a '{cols_lower[col_key]}')")
        else:
            cols_lower[col_key] = col
    
    # Terceira passada: verificação final de segurança
    attempts = 0
    while df.columns.duplicated().any() and attempts < 5:
        attempts += 1
        st.warning(f"⚠️ Tentativa {attempts}: Ainda há duplicatas - aplicando correção")
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
    
    if df.columns.duplicated().any():
        st.error("❌ Não foi possível remover todas as duplicatas!")
        return None
    
    final_shape = df.shape
    if original_shape != final_shape:
        st.info(f"📊 Formato alterado: {original_shape} → {final_shape}")
    
    return df

@st.cache_data(ttl=300)
def process_visualization_file(file_content, file_name, file_type):
    """Processa arquivo de visualização com cache"""
    try:
        # Debug: Verificar se é arquivo gerado pelo nosso sistema
        if 'geocoded_data_' in file_name:
            st.info(f"🔍 Arquivo detectado como gerado pelo sistema: {file_name}")
        
        # Ler arquivo baseado no tipo
        if file_name.endswith('.csv'):
            df_viz = pd.read_csv(BytesIO(file_content))
        elif file_name.endswith('.geojson'):
            # Criar arquivo temporário e garantir que seja fechado corretamente
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.geojson') as tmp_file:
                tmp_file.write(file_content)
                tmp_file_path = tmp_file.name
            
            try:
                # Ler o arquivo GeoJSON
                gdf = gpd.read_file(tmp_file_path)
                
                # IMPORTANTE: Verificar o que realmente vem do GeoPandas
                st.info(f"🔍 Debug GeoJSON: {len(gdf)} registros, colunas: {list(gdf.columns)}")
                
                # Converter para DataFrame regular (sem geometry)
                df_viz = pd.DataFrame(gdf.drop(columns='geometry'))
                
                # VERIFICAR SE JÁ TEM COORDENADAS DUPLICADAS
                has_lat = 'latitude' in df_viz.columns
                has_lon = 'longitude' in df_viz.columns
                
                st.info(f"🔍 Após remoção geometry: latitude={has_lat}, longitude={has_lon}")
                st.info(f"🔍 Colunas atuais: {list(df_viz.columns)}")
                
                # Se NÃO tem coordenadas OU tem duplicadas, extrair da geometria
                if not (has_lat and has_lon) or df_viz.columns.duplicated().any():
                    st.info("🔄 Extraindo coordenadas da geometria...")
                    
                    # Remover qualquer coluna latitude/longitude existente primeiro
                    columns_to_remove = ['latitude', 'longitude']
                    for col in columns_to_remove:
                        if col in df_viz.columns:
                            df_viz = df_viz.drop(columns=[col])
                            st.info(f"✅ Coluna '{col}' removida antes de extrair coordenadas")
                    
                    # Extrair coordenadas limpas da geometria
                    coords = []
                    for geom in gdf.geometry:
                        if geom.geom_type == 'Point':
                            coords.append((geom.y, geom.x))  # (lat, lon)
                        elif geom.geom_type in ['LineString', 'Polygon']:
                            centroid = geom.centroid
                            coords.append((centroid.y, centroid.x))
                        else:
                            coords.append((None, None))
                    
                    # Adicionar coordenadas limpas
                    df_viz['latitude'] = [coord[0] for coord in coords]
                    df_viz['longitude'] = [coord[1] for coord in coords]
                    
                    st.success(f"✅ Coordenadas extraídas: {len([c for c in coords if c[0] is not None])} válidas")
                else:
                    st.success("✅ Coordenadas já presentes e válidas no GeoJSON")
                
            finally:
                # Sempre deletar o arquivo temporário
                try:
                    os.unlink(tmp_file_path)
                except (OSError, FileNotFoundError):
                    pass
        else:
            df_viz = pd.read_excel(BytesIO(file_content))
        
        # APLICAR LIMPEZA ROBUSTA DE DUPLICATAS
        df_viz = remove_duplicate_columns_robust(df_viz, file_name)
        
        if df_viz is None:
            raise ValueError("Não foi possível processar o arquivo após remoção de duplicatas")
        
        # Verificação final de segurança
        if df_viz.columns.duplicated().any():
            raise ValueError(f"Ainda há colunas duplicadas após limpeza: {df_viz.columns[df_viz.columns.duplicated()].tolist()}")
        
        st.success(f"✅ Arquivo processado com sucesso: {len(df_viz)} registros, {len(df_viz.columns)} colunas")
        return df_viz
        
    except Exception as e:
        error_msg = str(e)
        if "Duplicate column names found" in error_msg:
            st.error("❌ Erro de colunas duplicadas detectado!")
            st.markdown("""
            **🔧 Solução:** O arquivo contém colunas com nomes idênticos. Isso geralmente acontece quando:
            1. O arquivo foi processado múltiplas vezes
            2. Houve erro na geração do arquivo original
            3. O arquivo GeoJSON contém coordenadas duplicadas
            
            **💡 Como corrigir:**
            - Reprocesse os dados originais
            - Use a função "Processamento CNPJ" para gerar um novo arquivo
            - Verifique se o arquivo não foi corrompido durante o download
            """)
        else:
            st.error(f"❌ Erro ao processar arquivo: {error_msg}")
        
        return None

# Página de visualização SIMPLIFICADA
def show_data_visualization():
    """Mostra a página de visualização de dados - VERSÃO SIMPLIFICADA"""
    st.markdown('<div class="fade-in">', unsafe_allow_html=True)
    
    st.markdown("## Visualização de Dados")
    st.markdown("Faça upload dos arquivos processados na tela 'Processamento CNPJ' para visualizar no mapa.")
    
    # Upload SIMPLES de arquivo processado
    uploaded_file = st.file_uploader(
        "📁 Selecione o arquivo gerado pelo processamento CNPJ",
        type=['csv', 'geojson', 'xlsx'],
        help="Use os arquivos baixados da tela 'Processamento CNPJ'"
    )
    
    if uploaded_file:
        try:
            # LEITURA SIMPLES do arquivo
            if uploaded_file.name.endswith('.csv'):
                df_viz = pd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith('.geojson'):
                # Para GeoJSON, usar método direto
                import json
                geojson_data = json.loads(uploaded_file.read().decode('utf-8'))
                
                # Extrair dados das features
                records = []
                for feature in geojson_data['features']:
                    record = feature['properties'].copy()
                    # Adicionar coordenadas
                    if feature['geometry']['type'] == 'Point':
                        coords = feature['geometry']['coordinates']
                        record['longitude'] = coords[0]
                        record['latitude'] = coords[1]
                    records.append(record)
                
                df_viz = pd.DataFrame(records)
            else:
                df_viz = pd.read_excel(uploaded_file)
            
            # LIMPEZA SIMPLES de duplicatas
            if df_viz.columns.duplicated().any():
                df_viz = df_viz.loc[:, ~df_viz.columns.duplicated(keep='first')]
                st.info("✅ Colunas duplicadas removidas automaticamente")
            
            # Verificar se tem latitude e longitude
            if 'latitude' not in df_viz.columns or 'longitude' not in df_viz.columns:
                st.error("❌ Arquivo deve ter colunas 'latitude' e 'longitude'")
                st.info("💡 Use os arquivos gerados na tela 'Processamento CNPJ'")
                return
            
            # Filtrar registros válidos
            df_viz = df_viz.dropna(subset=['latitude', 'longitude'])
            
            if df_viz.empty:
                st.warning("⚠️ Nenhum registro com coordenadas válidas encontrado")
                return
            
            # ESTATÍSTICAS SIMPLES
            st.success(f"✅ Arquivo carregado: {len(df_viz)} registros com coordenadas")
                    
            # MAPA SIMPLES
            st.markdown("### 🗺️ Mapa dos Dados")
            
            # Centro do mapa
            center_lat = float(df_viz['latitude'].mean())
            center_lon = float(df_viz['longitude'].mean())
            
            # Criar mapa
            m = folium.Map(
                location=[center_lat, center_lon], 
                zoom_start=10,
                tiles='CartoDB positron'
            )
            
            # Adicionar marcadores
            for idx, row in df_viz.iterrows():
                # Popup simples
                registro_num = int(idx) + 1 if isinstance(idx, (int, float)) else len(df_viz) + 1
                popup_text = f"<b>Registro {registro_num}</b><br>"
                if 'endereco_completo' in row:
                    popup_text += f"Endereço: {row['endereco_completo']}<br>"
                popup_text += f"Coordenadas: {float(row['latitude']):.6f}, {float(row['longitude']):.6f}"
                
                folium.Marker(
                    location=[float(row['latitude']), float(row['longitude'])],
                    popup=folium.Popup(popup_text, max_width=300),
                    tooltip=f"Registro {registro_num}",
                    icon=folium.Icon(color='green', icon='map-pin', prefix='fa')
                ).add_to(m)
            
            # Mostrar mapa
            folium_static(m, width=1400, height=600)
                    
        except Exception as e:
            st.error(f"❌ Erro ao processar arquivo: {str(e)}")
            st.info("💡 Certifique-se de usar os arquivos gerados na tela 'Processamento CNPJ'")
    
    st.markdown('</div>', unsafe_allow_html=True)

def main():
    """Função principal"""
    # Carregar CSS
    load_custom_css()
    
    # Sidebar com navegação
    with st.sidebar:
        # Inicializar página atual
        if 'current_page' not in st.session_state:
            st.session_state['current_page'] = 'Home'
        
        # Botões de navegação
        pages = {
            'Home': {'name': 'Página Inicial'},
            'CNPJ': {'name': 'Processamento CNPJ'},
            'Visualization': {'name': 'Visualização de Dados'}
        }
        
        for page_key, page_info in pages.items():
            is_active = st.session_state['current_page'] == page_key
            if st.button(
                page_info['name'], 
                key=page_key,
                use_container_width=True,
                type="secondary" if not is_active else "primary"
            ):
                st.session_state['current_page'] = page_key
                st.rerun()
    
    # Conteúdo principal baseado na página selecionada
    if st.session_state['current_page'] == 'Home':
        create_homepage()
    elif st.session_state['current_page'] == 'CNPJ':
        show_cnpj_processing()
    elif st.session_state['current_page'] == 'Visualization':
        show_data_visualization()
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div class="footer">
        <p class="footer-text"><strong class="footer-brand">Desenvolvido por SISGDSOLAR | UFES</strong></p>
        <p class="footer-text">Versão 3.0 | 2025 | Sistema de Geocodificação Inteligente</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 