# TRAZABILIDAD

### Campos de la tabla


| **Campo**                 | **Descripción**                                                                                                                                                                                                                                         | **Formato en PLC** |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| CICLO_ACTUAL              | Es un contador que se incrementa por cada producto terminado producido. Sirve para numerar el producto y para sincronizar el insertado de datos en la base de datos, ya que si el número cambia significa que es un nuevo registro o producto terminado | Uint               |
| OF                        | La Orden de Fabricación indica a qué orden pertenece el producto                                                                                                                                                                                        | Dint               |
| CODIGO_PRODUCTO_TERMINADO | Código del producto terminado                                                                                                                                                                                                                           | String[15]         |
| CODIGO_POLO_1             | Son los códigos de cada polo que componente el producto terminado. Ejemplo, en un producto conformado por 2 polos solo se registrarán los campos CODIGO_POLO_1 Y CODIGO_POLO_2                                                                          | String[23]         |
| CODIGO_POLO_2             |                                                                                                                                                                                                                                                         | String[23]         |
| CODIGO_POLO_3             |                                                                                                                                                                                                                                                         | String[23]         |
| CODIGO_POLO_4             |                                                                                                                                                                                                                                                         | String[23]         |
| CODIGO_INSPECCION         | Indica el estado de ese producto.                                                                                                                                                                                                                       | Int                |
| TURNO                     |                                                                                                                                                                                                                                                         | SInt               |

## Descripción:

Registra los detalles de cada producto terminado que sale de la máquina.

## Condición de actualización: 

Debe chequearse en intervalos no mayor a 2 segundos de lo contrario se perderían algunos datos ya que este registro se actualiza por cada ciclo de la máquina.

# OEE

### Campos de la tabla


| **Campo**                | **Descripción**                                     | **Formato en PLC** |
| ------------------------ | --------------------------------------------------- | ------------------ |
| CANT_PARADAS             | Contador de paradas de máquina                      | UInt               |
| DownTime_Minutos         | Minutos en qué estuvo parada la máquina             | UInt               |
| DownTime_Minutos_Externo | Minutos que se la máquina paró por causas externas. | UInt               |
| Disponibilidad           | Cálculo de disponibilidad                           | Real               |
| Performance              | Cálculo de Performance                              | Real               |
| Calidad                  | Cálculo Calidad                                     | Real               |
| OEE                      | Cálculo OEE                                         | Real               |
| PorcentajeStop           | Porcentaje de paradas                               | Real               |
| PorcentajeScrap          | Porcentaje de Scrap                                 | Real               |
| OF                       | Orden de fabricación actual                         | DInt               |
| turno                    | Turno actual                                        | SInt               |

## Descripción

Registra datos de OEE calculados por el programa corriendo en el PLC. Estos datos pueden servir para poder generar un historial de OEE.

## Condición de actualización

Depende el nivel de muestreo que se requiera, es recomendable 60 minutos

# ESTADÍSTICA

### Campos de la tabla


| **Campo**                        | **Descripción** | **Formato en PLC** |
| -------------------------------- | --------------- | ------------------ |
| BuenasTotales                    |                 | DInt               |
| MalasTotales                     |                 | DInt               |
| MalasPor_E1_QR                   |                 | Int                |
| MalasPor_E1_InspeccionInicial    |                 | Int                |
| MalasPor_E3_VerificacionIntegral |                 | Int                |
| MalasPor_E6_RemacheYSoldadura    |                 | Int                |
| MalasPor_E10_PresenciaAcoples    |                 | Int                |
| MalasPor_E11_PoloNoUtilizado     |                 | Int                |
| MalasPor_E12_TestAlturaTermica   |                 | Int                |
| MalasPor_E14_InspeccionFinal     |                 | Int                |
| ProduccionFaltante               |                 | UDInt              |
| OF                               |                 | DInt               |
| Turno                            |                 | SInt               |

## Descripción

Registra datos estadísticos generados por el programa corriendo en el PLC. Estos datos pueden servir para poder generar un historial de estadísticas.

## Condición de actualización

Depende el nivel de muestreo que se requiera, es recomendable 60 minutos

# REGISTRO DE EVENTOS

### Campos de la tabla


| **Campo**               | **Descripción**                                      | **Formato en PLC** |
| ----------------------- | ---------------------------------------------------- | ------------------ |
| Categoria               | Categoría del evento, si es SEGURO, FALLA, Etc       | SInt               |
| ID                      | Id que identifica al evento                          | SInt               |
| CantidadEventos         | Contador de dicho evento                             | Int                |
| FechaYHora              | Fecha y Hora del primer evento ocurrido de ese tipo. | DTL                |
| Turno                   | Turno actual                                         | SInt               |
| TiempoSegundosAcumulado | Suma el tiempo en que estuvo en este estado.         | Int                |


## Descripción

El PLC lleva un contador de muchos de los eventos importantes que pueden ocurrir. El propósito de este proceso de registro es generar un historial de esos eventos.

## Condición de actualización

Depende el nivel de muestreo que se requiera, es recomendable 30 minutos