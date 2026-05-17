import threading, json, collections, time
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go

#Datos en memoria 
lock       = threading.Lock()
vacunacion = []     # dicts de eventos-vacunacion (datos acumulados por cantón/fecha)
decesos    = []     # dicts de eventos-decesos    (muertes_acumuladas ya es running total)
ts_vac     = []     # timestamps float de llegada para calcular tasa
ts_dec     = []     # timestamps float de llegada para calcular tasa


# Los mensajes Kafka de fact_vacunacion solo traen provincia_id.
# El dashboard trabaja exclusivamente con los datos del stream.


#Consumer Kafka
def run_consumer():
    from confluent_kafka import Consumer
    c = Consumer({
        "bootstrap.servers":  "localhost:9092",
        "group.id":           "dashboard-rt",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": "false",
    })
    
    c.subscribe(["eventos-vacunacion", "eventos-decesos"])
    print("[Consumer] escuchando tópicos eventos-vacunacion y eventos-decesos...")
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        datos = json.loads(msg.value().decode())
        ahora = time.time()
        with lock:
            if msg.topic() == "eventos-vacunacion":
                vacunacion.append(datos)
                ts_vac.append(ahora)
            else:
                decesos.append(datos)
                ts_dec.append(ahora)

threading.Thread(target=run_consumer, daemon=True).start()


# Helpers 
def _snapshot_canton(vac_list):
    """
    Devuelve el último registro recibido por cantón.
    Necesario porque los datos de fact_vacunacion son acumulados:
    para cada cantón solo nos interesa el registro más reciente (mayor date_id y mayor dosis_total por el acumulado).
    """
    ultimos = {}
    for d in vac_list:
        cid = d.get("canton_id")
        if cid is None:
            continue
        if cid not in ultimos or d.get("dosis_total", 0) > ultimos[cid].get("dosis_total", 0):
            ultimos[cid] = d
    return list(ultimos.values())


def _tasa_msg(timestamps, ventana=10):
    """Mensajes recibidos en los últimos `ventana` segundos -> msg/s."""
    ahora = time.time()
    recientes = sum(1 for t in timestamps if ahora - t <= ventana)
    return round(recientes / ventana, 2)


def _nombre_prov(pid):
    return f"Prov. {pid}"


def kpi_box(label, valor, color="#222"):
    return html.Div([
        html.P(label,  style={"margin": 0, "fontSize": "11px", "color": "gray"}),
        html.H3(valor, style={"margin": 0, "color": color}),
    ], style={
        "border": "1px solid #ddd", "borderRadius": "8px",
        "padding": "10px 20px", "textAlign": "center", "minWidth": "130px",
    })


#Layout
app = dash.Dash(__name__)
app.title = "RT Dashboard — Vacunación Ecuador"

TABLA_STYLE = {
    "width": "100%", "borderCollapse": "collapse",
    "fontSize": "12px", "fontFamily": "sans-serif",
}
TH_STYLE = {
    "padding": "6px 10px", "borderBottom": "2px solid #ddd",
    "textAlign": "left", "fontWeight": "600", "color": "#555",
}
TD_STYLE = {"padding": "5px 10px", "borderBottom": "1px solid #f0f0f0"}

app.layout = html.Div([
    html.H2("Dashboard en Tiempo Real — Vacunación Ecuador",
            style={"textAlign": "center", "marginBottom": "20px", "fontSize": "18px"}),

    #KPIs 
    html.Div(id="kpis", style={
        "display": "flex", "gap": "12px",
        "justifyContent": "center", "flexWrap": "wrap", "marginBottom": "20px",
    }),

    #Fila 1: dosis por tipo | decesos en el tiempo 
    html.Div([
        dcc.Graph(id="g-dosis",   style={"flex": 1}),
        dcc.Graph(id="g-decesos", style={"flex": 1}),
    ], style={"display": "flex", "gap": "10px", "marginBottom": "10px"}),

    #Fila 2: top provincias | mensajes acumulados
    html.Div([
        dcc.Graph(id="g-provincia", style={"flex": 1}),
        dcc.Graph(id="g-flujo",     style={"flex": 1}),
    ], style={"display": "flex", "gap": "10px", "marginBottom": "20px"}),

    #Últimos 10 registros recibidos
    html.Div([
        html.H4("Últimos 10 registros recibidos",
                style={"fontSize": "13px", "fontWeight": "600",
                       "marginBottom": "10px", "color": "#333"}),
        html.Div(id="tabla-ultimos"),
    ], style={
        "padding": "16px", "border": "1px solid #e8e8e8",
        "borderRadius": "8px", "marginBottom": "20px",
    }),

    dcc.Interval(id="tick", interval=5000),   # refresca cada 5 s
], style={"maxWidth": "1400px", "margin": "0 auto", "padding": "20px"})


#Callback principal
@app.callback(
    Output("kpis",          "children"),
    Output("g-dosis",       "figure"),
    Output("g-decesos",     "figure"),
    Output("g-provincia",   "figure"),
    Output("g-flujo",       "figure"),
    Output("tabla-ultimos", "children"),
    Input("tick",           "n_intervals"),
)
def actualizar(_):
    with lock:
        vac  = list(vacunacion)
        dec  = list(decesos)
        tvac = list(ts_vac)
        tdec = list(ts_dec)

    snapshot = _snapshot_canton(vac)

    #KPIs
    #Datos acumulados: snapshot ya tiene el último valor por cantón -> suma correcta
    total_dosis   = sum(d.get("dosis_total", 0)          for d in snapshot)
    total_muertes = max((d.get("muertes_acumuladas", 0)  for d in dec), default=0)
    tasa          = _tasa_msg(tvac)

    kpis = [
        kpi_box("Eventos recibidos (vac.)",  f"{len(vac):,}"),
        kpi_box("Dosis totales (acumulado)", f"{total_dosis:,}",  "#1d6fa5"),
        kpi_box("Eventos recibidos (dec.)",  f"{len(dec):,}"),
        kpi_box("Muertes (acumulado)",       f"{total_muertes:,}", "#c0392b"),
        kpi_box("Tasa (msg/s)",              f"{tasa}",            "#27ae60"),
    ]

    #G1: Dosis acumuladas por tipo 
    # Snapshot: último registro por cantón, luego se suman los acumulados por tipo
    tipos  = ["primera_dosis", "segunda_dosis", "dosis_unica", "dosis_refuerzo"]
    etiq   = ["Primera",       "Segunda",       "Única",       "Refuerzo"]
    colores= ["#4e79a7",       "#f28e2b",       "#59a14f",     "#e15759"]
    vals1  = [sum(d.get(t, 0) for d in snapshot) for t in tipos]
    g1 = go.Figure(go.Bar(
        x=etiq, y=vals1, marker_color=colores,
        text=[f"{v:,}" for v in vals1], textposition="outside",
    ))
    g1.update_layout(
        title="Dosis acumuladas por tipo",
        yaxis_title="Dosis", margin=dict(t=50, b=20, l=60),
        yaxis=dict(rangemode="tozero"),
    )

    #G2: Decesos acumulados en el tiempo 
    # muertes_acumuladas ya es un running total -> se grafica directo
    g2 = go.Figure()
    if dec:
        idx = list(range(len(dec)))
        g2.add_trace(go.Scatter(
            x=idx, y=[d.get("muertes_acumuladas", 0) for d in dec],
            mode="lines", name="Acumuladas",
            line=dict(color="#e15759", width=2),
            fill="tozeroy", fillcolor="rgba(225,87,89,0.1)",
        ))
    g2.update_layout(
        title="Decesos acumulados en el tiempo",
        yaxis_title="Muertes acumuladas",
        margin=dict(t=50, b=20), xaxis_title="Mensaje #",
    )

    #G3: Top 10 provincias — dosis totales (acumuladas) 
    conteo_prov = collections.Counter()
    for d in snapshot:
        nombre = _nombre_prov(d.get("provincia_id", "?"))
        conteo_prov[nombre] += d.get("dosis_total", 0)
    top = conteo_prov.most_common(10)
    g3 = go.Figure()
    if top:
        nombres, vals3 = zip(*top)
        g3.add_trace(go.Bar(
            x=list(vals3), y=list(nombres), orientation="h",
            marker_color="#4e79a7",
            text=[f"{v:,}" for v in vals3], textposition="outside",
        ))
        g3.update_layout(yaxis=dict(autorange="reversed"))
    g3.update_layout(
        title="Top 10 provincias — dosis totales",
        xaxis_title="Dosis", margin=dict(t=50, b=20, r=70),
    )

    #G4: Mensajes acumulados + tasa 
    #y = range(1, n+1) -> conteo real acumulado, no índices de posición
    g4 = go.Figure()
    if vac:
        g4.add_trace(go.Scatter(
            x=list(range(len(vac))),
            y=list(range(1, len(vac) + 1)),
            mode="lines", name="Vacunación",
            line=dict(color="#59a14f", width=2),
            fill="tozeroy", fillcolor="rgba(89,161,79,0.12)",
        ))
    if dec:
        g4.add_trace(go.Scatter(
            x=list(range(len(dec))),
            y=list(range(1, len(dec) + 1)),
            mode="lines", name="Decesos",
            line=dict(color="#e15759", width=1.5, dash="dot"),
        ))
    g4.update_layout(
        title=f"Mensajes acumulados — tasa actual: {tasa} msg/s",
        yaxis_title="Mensajes recibidos",
        xaxis_title="Secuencia",
        legend=dict(x=0, y=1),
        margin=dict(t=50, b=20),
    )

    #Tabla: últimos 10 registros recibidos 
    #Mezcla los últimos 5 de cada tópico, mostrando los más recientes primero
    ultimos_vac = list(reversed(vac[-5:])) if vac else []
    ultimos_dec = list(reversed(dec[-5:])) if dec else []

    def fila_vac(d):
        return html.Tr([
            html.Td("VACUNACIÓN",
                    style={**TD_STYLE, "color": "#1d6fa5", "fontWeight": "600"}),
            html.Td(str(d.get("canton_id",   "—")), style=TD_STYLE),
            html.Td(str(d.get("date_id",     "—")), style=TD_STYLE),
            html.Td(f"{d.get('dosis_total', 0):,}", style=TD_STYLE),
            html.Td(f"{d.get('primera_dosis', 0):,}", style=TD_STYLE),
            html.Td("—", style=TD_STYLE),
            html.Td("—", style=TD_STYLE),
        ])

    def fila_dec(d):
        return html.Tr([
            html.Td("DECESOS",
                    style={**TD_STYLE, "color": "#c0392b", "fontWeight": "600"}),
            html.Td("—", style=TD_STYLE),
            html.Td(str(d.get("date_id", "—")), style=TD_STYLE),
            html.Td("—", style=TD_STYLE),
            html.Td("—", style=TD_STYLE),
            html.Td(f"{d.get('muertes_acumuladas', 0):,}", style=TD_STYLE),
            html.Td(f"{d.get('muertes_diarias',    0):,}", style=TD_STYLE),
        ])

    tabla = html.Table([
        html.Thead(html.Tr([
            html.Th("Tópico",           style=TH_STYLE),
            html.Th("Cantón ID",        style=TH_STYLE),
            html.Th("Fecha ID",         style=TH_STYLE),
            html.Th("Dosis total",      style=TH_STYLE),
            html.Th("1ª Dosis",         style=TH_STYLE),
            html.Th("Muertes acum.",    style=TH_STYLE),
            html.Th("Muertes diarias",  style=TH_STYLE),
        ])),
        html.Tbody(
            [fila_vac(d) for d in ultimos_vac] +
            [fila_dec(d) for d in ultimos_dec]
        ),
    ], style=TABLA_STYLE)

    if not vac and not dec:
        tabla = html.P("Sin datos aún — esperando mensajes del broker...",
                       style={"color": "gray", "fontSize": "12px"})

    return kpis, g1, g2, g3, g4, tabla


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
