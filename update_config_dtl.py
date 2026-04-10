#!/usr/bin/env python3
"""
Script para actualizar la configuración usando componentes DTL individuales
en lugar del objeto FechaYHora completo
"""

import json
import re

def load_current_config():
    """Cargar configuración actual"""
    with open("config_tablas_especializadas.json", "r", encoding="utf-8") as f:
        return json.load(f)

def load_tags_mapping():
    """Cargar mapeo de tags actual"""
    with open("tags.json", "r", encoding="utf-8") as f:
        return json.load(f)

def find_dtl_components(tags_mapping, base_event):
    """Encontrar componentes DTL para un evento"""
    components = {}
    base_path = f"OPC_DATOS.REGISTRO_EVENTOS.FALLAS.{base_event}"
    
    # Buscar componentes DTL
    dtl_components = ["YEAR", "MONTH", "DAY", "WEEKDAY", "HOUR", "MINUTE"]
    
    for component in dtl_components:
        tag_path = f"{base_path}.FechaYHora.{component}"
        if tag_path in tags_mapping:
            components[component] = tag_path
    
    return components

def update_eventos_config(config, tags_mapping):
    """Actualizar configuración de eventos con componentes DTL"""
    eventos_config = config["tablas"]["eventos"]
    new_tags = []
    
    # Procesar tags actuales
    for tag in eventos_config["tags"]:
        if ".FechaYHora" in tag and not any(comp in tag for comp in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]):
            # Es un tag FechaYHora completo, reemplazar con componentes
            base_event = tag.split(".FechaYHora")[0].split("OPC_DATOS.REGISTRO_EVENTOS.FALLAS.")[-1]
            
            # Encontrar componentes DTL
            components = find_dtl_components(tags_mapping, base_event)
            
            if components:
                print(f"Reemplazando {tag}")
                print(f"  Componentes encontrados: {list(components.keys())}")
                
                # Agregar componentes DTL en orden específico
                for comp in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]:
                    if comp in components:
                        new_tags.append(components[comp])
                print(f"  Tags agregados: {len([c for c in ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE'] if c in components])}")
            else:
                print(f"⚠️  No se encontraron componentes DTL para {tag}")
                # Mantener el tag original si no hay componentes
                new_tags.append(tag)
        else:
            # Mantener otros tags
            new_tags.append(tag)
    
    # Actualizar configuración
    eventos_config["tags"] = new_tags
    return config

def main():
    print("=== Actualizando Configuración para Componentes DTL ===\n")
    
    # Cargar archivos
    config = load_current_config()
    tags_mapping = load_tags_mapping()
    
    print(f"Tags en eventos antes: {len(config['tablas']['eventos']['tags'])}")
    
    # Actualizar configuración
    updated_config = update_eventos_config(config, tags_mapping)
    
    print(f"\nTags en eventos después: {len(updated_config['tablas']['eventos']['tags'])}")
    
    # Guardar nueva configuración
    backup_file = "config_tablas_especializadas_backup.json"
    with open(backup_file, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Backup guardado en: {backup_file}")
    
    with open("config_tablas_especializadas.json", "w", encoding="utf-8") as f:
        json.dump(updated_config, f, indent=2, ensure_ascii=False)
    print(f"✅ Configuración actualizada guardada")
    
    # Mostrar resumen de cambios
    print("\n=== Resumen de Cambios ===")
    eventos_tags = updated_config['tablas']['eventos']['tags']
    dtl_tags = [tag for tag in eventos_tags if any(comp in tag for comp in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"])]
    print(f"Tags DTL agregados: {len(dtl_tags)}")
    
    # Agrupar por evento
    eventos_dtl = {}
    for tag in dtl_tags:
        event_match = re.search(r'FALLAS\.([^\.]+)\.FechaYHora\.(\w+)', tag)
        if event_match:
            event_name = event_match.group(1)
            component = event_match.group(2)
            if event_name not in eventos_dtl:
                eventos_dtl[event_name] = []
            eventos_dtl[event_name].append(component)
    
    print(f"\nEventos con componentes DTL:")
    for event, components in eventos_dtl.items():
        print(f"  {event}: {', '.join(components)}")

if __name__ == "__main__":
    main()
