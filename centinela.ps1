#requires -Version 5.1
<#
.SYNOPSIS
    Centinela Urbano desde Windows: configura, levanta y opera el sistema en una o varias PCs.

.DESCRIPTION
    Solo necesita Docker Desktop. No hace falta bash, WSL a mano ni máquinas Linux.
    Lo normal es usarlo con el lanzador centinela.cmd (doble clic o desde cmd/PowerShell):

        centinela.cmd configurar     pregunta el modo (una PC o varias), detecta la IP y revisa puertos
        centinela.cmd iniciar        levanta lo que corresponde a esta PC y espera a que esté sano
        centinela.cmd estado         contenedores, salud de cada nodo y conectividad entre PCs

    "centinela.cmd ayuda" muestra todos los comandos.

.NOTES
    Compatible con Windows PowerShell 5.1 (el que trae Windows 10/11) y PowerShell 7.
    El archivo se guarda en UTF-8 con BOM para que PowerShell 5.1 lea bien las tildes.
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)] [string] $Comando = 'menu',
    [Parameter(Position = 1)] [string] $Objetivo,
    [string] $Nodo,
    [string] $IpA,
    [string] $IpB,
    [string] $Generadores,
    [switch] $Workers,
    [switch] $SinWorkers,
    [switch] $Arreglar,
    [switch] $Compilar,
    [switch] $Borrar,
    [switch] $Quitar,
    [switch] $Si,
    [int] $Zona = 0,
    [string] $Tipo,
    [string] $Velocidad = '1',
    [int] $Multiplicador = 20,
    [int] $Segundos = 60,
    [switch] $IncluirPermanentes,
    [switch] $Pausa
)

Set-StrictMode -Version 1.0
$ErrorActionPreference = 'Continue'   # con 'Stop', PowerShell 5.1 corta en cualquier línea que docker escriba en stderr
$ProgressPreference = 'SilentlyContinue'
# La salida de docker viene en UTF-8; sin esto PowerShell 5.1 la muestra con caracteres raros
try { [Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false) } catch { }
# Todas las llamadas HTTP del script son a la red local: un proxy del sistema sin excepción para la LAN
# (común en laboratorios) las haría fallar. PowerShell 5.1 usa WebRequest; PowerShell 7, HttpClient.
try { [System.Net.WebRequest]::DefaultWebProxy = New-Object System.Net.WebProxy } catch { }
try { [System.Net.Http.HttpClient]::DefaultProxy = New-Object System.Net.WebProxy } catch { }

$Raiz = $PSScriptRoot
$ArchivoEnv = Join-Path $Raiz '.env'
$ArchivoEjemplo = Join-Path $Raiz '.env.example'
$DirLocal = Join-Path $Raiz '.centinela'
$ArchivoLocal = Join-Path $DirLocal 'local.json'
$EsWindows = ($env:OS -eq 'Windows_NT')
$GrupoFirewall = 'Centinela Urbano'
$global:LASTEXITCODE = 0   # con StrictMode, leerla antes de que corra un programa externo da error

# Ruta de docker.exe. Se busca CON extensión a propósito: si en el PATH hay antes un archivo "docker"
# sin extensión (p. ej. creado por error en C:\Windows\System32), PowerShell intentaría abrirlo como
# documento ("¿Con qué aplicación desea abrir 'docker'?") en lugar de ejecutar Docker.
$DockerExe = 'docker'
if ($EsWindows) {
    $DockerExe = $null
    $c = Get-Command -Name 'docker.exe' -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($c) { $DockerExe = $c.Source }
    else {
        foreach ($d in @((Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'), (Join-Path $env:USERPROFILE '.docker\bin\docker.exe'))) {
            if (Test-Path -LiteralPath $d) { $DockerExe = $d; break }
        }
    }
}

# Puertos publicados en la PC y su valor por defecto (los mismos de los docker-compose)
$PuertosPorDefecto = [ordered]@{
    SIMULATOR_PORT = 8080; INGESTION_PORT = 8081; CORRELATION_PORT = 8082; ALERT_SERVICE_PORT = 8083
    PROMETHEUS_PORT = 9090; GRAFANA_PORT = 3000; DB_PORT = 5432; REDIS_PORT = 6379
    KAFKA_PORT = 9092; KAFKA_EXTERNAL_PORT = 29092; WORKER_INGESTION_PORT = 8081; WORKER_CORRELATION_PORT = 8082
}

# Memoria mínima recomendada para la VM de Docker Desktop, en GB
$MemoriaPorRol = @{ UNICO = 6; A = 4.5; B = 3; C = 1.5; D = 1.5 }

$ImagenesBase = @('apache/kafka:3.9.1', 'timescale/timescaledb:2.17.2-pg16', 'redis:7.4-alpine', 'grafana/grafana:11.4.0', 'prom/prometheus:v3.1.0')
$ImagenesPropias = @('centinela/sensor-simulator:1.0.0', 'centinela/ingestion-service:1.0.0', 'centinela/correlation-engine:1.0.0', 'centinela/alert-service:1.0.0')

# =============================================================================================
# Salida por consola
# =============================================================================================
function Write-Titulo([string]$Texto) { Write-Host ''; Write-Host "== $Texto" -ForegroundColor Cyan }
function Write-Ok([string]$Texto) { Write-Host "  [OK] $Texto" -ForegroundColor Green }
function Write-Aviso([string]$Texto) { Write-Host "  [!]  $Texto" -ForegroundColor Yellow }
function Write-Fallo([string]$Texto) { Write-Host "  [X]  $Texto" -ForegroundColor Red }
function Write-Info([string]$Texto) { Write-Host "       $Texto" }

function Stop-Centinela([string]$Texto) {
    Write-Fallo $Texto
    if ($Pausa) { Read-Host 'Presione Enter para cerrar' | Out-Null }
    exit 1
}

function Read-SiNo([string]$Pregunta, [bool]$PorDefecto = $true) {
    if ($Si) { return $true }
    $sufijo = if ($PorDefecto) { '[S/n]' } else { '[s/N]' }
    $r = Read-Host "  $Pregunta $sufijo"
    if ([string]::IsNullOrWhiteSpace($r)) { return $PorDefecto }
    return ($r.Trim().ToLower() -in @('s', 'si', 'sí', 'y', 'yes'))
}

function Read-Valor([string]$Pregunta, [string]$PorDefecto) {
    if ($Si) { return $PorDefecto }
    $r = Read-Host "  $Pregunta [$PorDefecto]"
    if ([string]::IsNullOrWhiteSpace($r)) { return $PorDefecto }
    return $r.Trim()
}

# =============================================================================================
# Archivo .env (UTF-8 sin BOM: docker compose no reconoce la primera variable si lleva BOM)
# =============================================================================================
function Read-Env([string]$Ruta) {
    $valores = [ordered]@{}
    if (-not (Test-Path -LiteralPath $Ruta)) { return $valores }
    foreach ($linea in [IO.File]::ReadAllLines($Ruta)) {
        $l = $linea.Trim().TrimStart([char]0xFEFF)
        if ($l -eq '' -or $l.StartsWith('#')) { continue }
        $i = $l.IndexOf('=')
        if ($i -lt 1) { continue }
        $k = $l.Substring(0, $i).Trim()
        $v = $l.Substring($i + 1).Trim()
        if ($v.Length -ge 2 -and (($v.StartsWith('"') -and $v.EndsWith('"')) -or ($v.StartsWith("'") -and $v.EndsWith("'")))) {
            $v = $v.Substring(1, $v.Length - 2)
        }
        $valores[$k] = $v
    }
    return $valores
}

function Save-Env([System.Collections.IDictionary]$Cambios) {
    if (-not (Test-Path -LiteralPath $ArchivoEnv)) { Copy-Item -LiteralPath $ArchivoEjemplo -Destination $ArchivoEnv }
    $lineas = New-Object System.Collections.Generic.List[string]
    foreach ($l in [IO.File]::ReadAllLines($ArchivoEnv)) { $lineas.Add($l.TrimStart([char]0xFEFF)) }
    $pendientes = [ordered]@{}
    foreach ($k in $Cambios.Keys) { $pendientes[$k] = [string]$Cambios[$k] }
    for ($i = 0; $i -lt $lineas.Count; $i++) {
        if ($lineas[$i] -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=') {
            $k = $Matches[1]
            if ($pendientes.Contains($k)) { $lineas[$i] = "$k=$($pendientes[$k])"; $pendientes.Remove($k) }
        }
    }
    foreach ($k in $pendientes.Keys) { $lineas.Add("$k=$($pendientes[$k])") }
    $sinBom = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($ArchivoEnv, (($lineas -join "`n").TrimEnd() + "`n"), $sinBom)
}

function Get-Config {
    $cfg = Read-Env $ArchivoEjemplo
    $real = Read-Env $ArchivoEnv
    foreach ($k in $real.Keys) { $cfg[$k] = $real[$k] }
    foreach ($k in $PuertosPorDefecto.Keys) { if (-not $cfg.Contains($k) -or -not $cfg[$k]) { $cfg[$k] = [string]$PuertosPorDefecto[$k] } }
    return $cfg
}

function Get-Puerto($Cfg, [string]$Variable) { return [int]$Cfg[$Variable] }

# Resumen de los valores que comparten todas las PCs (IPs, puertos, workers y llave): dos PCs con distinta
# huella no se van a entender (puertos distintos, otra llave de seudonimizacion)
function Get-HuellaConfig($Cfg) {
    $claves = @('NODE_A_IP', 'NODE_B_IP', 'GENERATOR_IPS', 'WORKERS', 'PLATE_HMAC_SECRET') + @($PuertosPorDefecto.Keys)
    $texto = ($claves | ForEach-Object { '{0}={1}' -f $_, ([string]$Cfg[$_]).Trim() }) -join "`n"
    $hash = [System.Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($texto))
    return (($hash[0..3] | ForEach-Object { $_.ToString('X2') }) -join '')
}

# =============================================================================================
# Roles de esta PC: UNICO (todo en una), o A / B / C / D (varias PCs)
# =============================================================================================
function Get-Local {
    if (Test-Path -LiteralPath $ArchivoLocal) {
        try { return (Get-Content -LiteralPath $ArchivoLocal -Raw | ConvertFrom-Json) } catch { }
    }
    return $null
}

function Save-Local([string[]]$Roles) {
    if (-not (Test-Path -LiteralPath $DirLocal)) { New-Item -ItemType Directory -Path $DirLocal | Out-Null }
    $json = ([pscustomobject]@{ Roles = $Roles }) | ConvertTo-Json
    [IO.File]::WriteAllText($ArchivoLocal, $json, (New-Object System.Text.UTF8Encoding($false)))
}

# Roles guardados por "configurar" para esta PC
function Get-RolesLocales {
    $local = Get-Local
    if ($local -and $local.Roles) { return @($local.Roles) }
    return @()
}

# Roles sobre los que actúa el comando: -Nodo restringe (p. ej. "iniciar -Nodo A" en una PC con A y B)
function Get-Roles {
    if ($Nodo -and $Comando -ne 'configurar') {
        $pedidos = ConvertTo-Roles $Nodo
        $locales = Get-RolesLocales
        if ($Comando -in @('iniciar', 'detener', 'estado', 'logs', 'puertos', 'firewall')) {
            foreach ($r in $pedidos) { if ($locales -notcontains $r) { Stop-Centinela "El nodo $r no está configurado en esta PC (aquí: $($locales -join ', '))." } }
            return $pedidos
        }
    }
    return Get-RolesLocales
}

function ConvertTo-Roles([string]$Texto) {
    $t = $Texto.Trim().ToUpper()
    if ($t -in @('UNICO', 'ÚNICO', 'UNA', '1', 'TODO')) { return @('UNICO') }
    $roles = @($t -split '[,; ]+' | Where-Object { $_ } | ForEach-Object { $_ -replace '^NODO-?', '' })
    foreach ($r in $roles) { if ($r -notin @('A', 'B', 'C', 'D')) { Stop-Centinela "Nodo desconocido: '$r'. Use UNICO o A, B, C, D (se pueden combinar: A,B)." } }
    if (($roles -contains 'C') -and ($roles -contains 'D')) { Stop-Centinela 'C y D no pueden correr en la misma PC (usan los mismos puertos): elija uno.' }
    $orden = @('A', 'B', 'C', 'D')
    return @($orden | Where-Object { $roles -contains $_ })
}

# Llamar siempre como @(Get-Generadores $cfg): con una sola IP, PowerShell devuelve el texto suelto y $gens[0]
# seria su primer caracter ("1" en vez de "192.168.1.12")
function Get-Generadores($Cfg) { return @(([string]$Cfg['GENERATOR_IPS']) -split '[,; ]+' | Where-Object { $_ }) }

function Test-Workers($Cfg) { return ([string]$Cfg['WORKERS']).Trim().ToLower() -in @('true', 'si', 'sí', '1', 'yes') }

# -Velocidad acepta "0,5" o "0.5": en Windows en español la coma es el separador decimal
function Get-Velocidad {
    $texto = ([string]$Velocidad).Trim().Replace(',', '.')
    $v = 0.0
    $ok = [double]::TryParse($texto, [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$v)
    if (-not $ok -or $v -lt 0.1 -or $v -gt 10) { Stop-Centinela "Velocidad inválida: '$Velocidad' (use un valor entre 0,1 y 10)." }
    return $v.ToString([Globalization.CultureInfo]::InvariantCulture)
}

# Zonas de cada generador: C (índice 0) y D (índice 1) se reparten las 12 zonas
function Get-ZonasGenerador([int]$Indice, [int]$Total) {
    if ($Total -lt 1) { $Total = 1 }
    $porNodo = [math]::Ceiling(12 / $Total)
    $ini = $Indice * $porNodo + 1
    $fin = [math]::Min(12, ($Indice + 1) * $porNodo)
    if ($ini -gt 12) { return '1,2,3,4,5,6,7,8,9,10,11,12' }
    return (($ini..$fin) -join ',')
}

# Datos de docker compose para cada rol
function Get-InfoRol([string]$Rol, $Cfg) {
    switch ($Rol) {
        'UNICO' { return @{ Archivo = [IO.Path]::Combine($Raiz, 'docker-compose.yml'); Proyecto = 'centinela'; Perfiles = @(); Entorno = @{} } }
        'A' { return @{ Archivo = [IO.Path]::Combine($Raiz, 'deploy', 'nodo-a', 'docker-compose.yml'); Proyecto = 'centinela-nodo-a'; Perfiles = @(); Entorno = @{} } }
        'B' { return @{ Archivo = [IO.Path]::Combine($Raiz, 'deploy', 'nodo-b', 'docker-compose.yml'); Proyecto = 'centinela-nodo-b'; Perfiles = @(); Entorno = @{} } }
        default {
            $indice = if ($Rol -eq 'C') { 0 } else { 1 }
            # Solo existen dos roles generadores (C y D): con una IP, C simula las 12 zonas
            $total = if ((Get-Generadores $Cfg).Count -le 1) { 1 } else { 2 }
            $perfiles = @()
            if (Test-Workers $Cfg) { $perfiles = @('--profile', 'workers') }
            return @{
                Archivo  = [IO.Path]::Combine($Raiz, 'deploy', 'nodo-c', 'docker-compose.yml')
                Proyecto = 'centinela-nodo-c'
                Perfiles = $perfiles
                # Las variables del proceso tienen prioridad sobre el .env compartido
                Entorno  = @{ SIMULATOR_ZONES = (Get-ZonasGenerador $indice $total); SIMULATOR_NODE_NAME = "nodo-$($Rol.ToLower())" }
            }
        }
    }
}

# Variables de puerto que publica cada rol en esta PC
function Get-VariablesPuerto([string]$Rol, $Cfg) {
    switch ($Rol) {
        'UNICO' { return @('SIMULATOR_PORT', 'INGESTION_PORT', 'CORRELATION_PORT', 'ALERT_SERVICE_PORT', 'PROMETHEUS_PORT', 'GRAFANA_PORT', 'DB_PORT', 'REDIS_PORT', 'KAFKA_EXTERNAL_PORT') }
        'A' { return @('KAFKA_PORT', 'DB_PORT', 'REDIS_PORT') }
        'B' { return @('INGESTION_PORT', 'CORRELATION_PORT', 'ALERT_SERVICE_PORT', 'PROMETHEUS_PORT', 'GRAFANA_PORT') }
        default {
            if (Test-Workers $Cfg) { return @('SIMULATOR_PORT', 'WORKER_INGESTION_PORT', 'WORKER_CORRELATION_PORT') }
            return @('SIMULATOR_PORT')
        }
    }
}

# =============================================================================================
# Docker
# =============================================================================================
function Test-DockerCorriendo {
    $v = & $DockerExe info --format '{{.ServerVersion}}' 2>$null
    return ($LASTEXITCODE -eq 0 -and $v)
}

# Con Docker Desktop en modo de contenedores Windows, ninguna imagen del proyecto (Linux) puede correr
function Assert-ContenedoresLinux {
    $tipo = & $DockerExe info --format '{{.OSType}}' 2>$null
    if ($tipo -eq 'windows') {
        Stop-Centinela 'Docker Desktop está en modo de contenedores Windows: clic derecho en el ícono de Docker > "Switch to Linux containers" y repita.'
    }
}

function Assert-Docker {
    if (-not $DockerExe -or -not (Get-Command $DockerExe -ErrorAction SilentlyContinue)) {
        Stop-Centinela 'No se encontró Docker. Instale Docker Desktop (https://www.docker.com/products/docker-desktop/) y vuelva a intentar (si lo acaba de instalar, abra una terminal nueva).'
    }
    if (Test-DockerCorriendo) { Assert-ContenedoresLinux; return }
    Write-Aviso 'Docker Desktop no está corriendo; intento abrirlo...'
    & $DockerExe desktop start 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0 -and $EsWindows) {
        foreach ($exe in @((Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'), (Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\Docker Desktop.exe'))) {
            if (Test-Path -LiteralPath $exe) { Start-Process -FilePath $exe; break }
        }
    }
    for ($i = 0; $i -lt 60; $i++) {
        if (Test-DockerCorriendo) { Write-Ok 'Docker Desktop listo'; Assert-ContenedoresLinux; return }
        Start-Sleep -Seconds 3
    }
    Stop-Centinela 'Docker Desktop no respondió en 3 minutos. Ábralo a mano, espere a que diga "Engine running" y repita.'
}

# Diferencia en segundos entre el reloj de Windows y el de la VM de Docker (se atrasa tras suspender la PC).
# Usa una imagen que ya esté en esta PC: una descarga dentro de la medición daría un falso desfase.
function Get-DesfaseVm {
    $imagen = $null
    foreach ($i in @('redis:7.4-alpine') + $ImagenesPropias + @('apache/kafka:3.9.1', 'timescale/timescaledb:2.17.2-pg16')) {
        $id = & $DockerExe images -q $i 2>$null
        if ($LASTEXITCODE -eq 0 -and $id) { $imagen = $i; break }
    }
    if (-not $imagen) { return $null }
    $antes = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $vm = & $DockerExe run --rm --entrypoint date $imagen +%s 2>$null
    $despues = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    if ($LASTEXITCODE -ne 0 -or -not ($vm -match '^\d+$')) { return $null }
    return [math]::Round([double]$vm - (($antes + $despues) / 2000.0), 1)
}

# Diferencia en segundos entre esta PC y otra, usando la cabecera Date de una respuesta HTTP
function Get-DesfaseRemoto([string]$Url) {
    try {
        $r = Invoke-WebRequest -Uri $Url -TimeoutSec 4 -UseBasicParsing -ErrorAction Stop
        $fecha = [DateTimeOffset]::Parse([string]$r.Headers['Date'], [Globalization.CultureInfo]::InvariantCulture)
        return [math]::Round(($fecha - [DateTimeOffset]::UtcNow).TotalSeconds, 0)
    } catch { return $null }
}

function Show-Relojes($Cfg) {
    $d = Get-DesfaseVm
    if ($null -eq $d) { Write-Aviso 'No pude leer el reloj de la máquina virtual de Docker.' }
    elseif ([math]::Abs($d) -le 2) { Write-Ok "Reloj de Docker sincronizado con Windows (desfase $d s)" }
    else {
        Write-Fallo "El reloj de Docker está desfasado $d s respecto de Windows: la ingesta puede rechazar eventos."
        Write-Info  'Arreglo: reiniciar Docker Desktop (o "wsl --shutdown" y abrirlo de nuevo).'
    }
    $roles = Get-RolesLocales
    if ($roles -notcontains 'UNICO' -and $roles -notcontains 'B' -and $Cfg['NODE_B_IP']) {
        $r = Get-DesfaseRemoto ('http://{0}:{1}/actuator/health/liveness' -f $Cfg['NODE_B_IP'], $Cfg['ALERT_SERVICE_PORT'])
        if ($null -ne $r) {
            if ([math]::Abs($r) -le 2) { Write-Ok "Reloj sincronizado con el nodo B (desfase $r s)" }
            else {
                Write-Fallo "Esta PC y el nodo B difieren $r s: sincronice la hora de las PCs."
                Write-Info  'Arreglo (cmd como administrador en cada PC): net start w32time  y  w32tm /resync /force'
            }
        }
    }
}

function Invoke-Compose([string]$Rol, $Cfg, [string[]]$Argumentos) {
    $info = Get-InfoRol $Rol $Cfg
    $base = @('compose', '--env-file', $ArchivoEnv, '-f', $info.Archivo, '-p', $info.Proyecto) + $info.Perfiles
    $previos = @{}
    foreach ($k in $info.Entorno.Keys) { $previos[$k] = [Environment]::GetEnvironmentVariable($k); [Environment]::SetEnvironmentVariable($k, $info.Entorno[$k]) }
    try {
        & $DockerExe @($base + $Argumentos) | Out-Host
        return $LASTEXITCODE
    } finally {
        foreach ($k in $previos.Keys) { [Environment]::SetEnvironmentVariable($k, $previos[$k]) }
    }
}

function Get-ComposeServicios([string]$Rol, $Cfg) {
    $info = Get-InfoRol $Rol $Cfg
    $salida = & $DockerExe compose --env-file $ArchivoEnv -f $info.Archivo -p $info.Proyecto ps --all --format json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $salida) { return @() }
    $texto = ($salida -join "`n").Trim()
    if ($texto.StartsWith('[')) { return @($texto | ConvertFrom-Json) }
    return @($salida | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json })
}

# Contenedor que publica el puerto y su proyecto de docker compose (centinela, centinela-nodo-a, ...)
function Get-ContenedorConPuerto([int]$Puerto) {
    $lineas = & $DockerExe ps --filter "publish=$Puerto" --format '{{.Names}}|{{.Label "com.docker.compose.project"}}' 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $lineas) { return $null }
    $partes = ([string]($lineas | Select-Object -First 1)).Split('|')
    return @{ Nombre = $partes[0]; Proyecto = $(if ($partes.Count -gt 1) { $partes[1] } else { '' }) }
}

# Rol de un proyecto de docker compose de Centinela (para detener otro modo que siga encendido)
function Get-RolDeProyecto([string]$Proyecto) {
    switch ($Proyecto) {
        'centinela' { return 'UNICO' }
        'centinela-nodo-a' { return 'A' }
        'centinela-nodo-b' { return 'B' }
        'centinela-nodo-c' { return 'C' }
        default { return $null }
    }
}

# =============================================================================================
# Red: IP de la PC, prueba TCP, HTTP
# =============================================================================================
function Get-IpLan {
    if (-not $EsWindows) {
        $ip = (& hostname -I 2>$null) -split ' ' | Where-Object { $_ -match '^\d+\.\d+\.\d+\.\d+$' -and $_ -notlike '127.*' -and $_ -notlike '172.1[7-9].*' } | Select-Object -First 1
        return $ip
    }
    $excluir = 'vEthernet|WSL|Hyper-V|Docker|VirtualBox|VMware|Loopback|Bluetooth|VPN|TAP|Tailscale|ZeroTier|WireGuard|Hamachi|Fortinet|AnyConnect|Npcap'
    try {
        $c = Get-NetIPConfiguration -ErrorAction Stop | Where-Object {
            $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq 'Up' -and $_.InterfaceAlias -notmatch $excluir -and $_.InterfaceDescription -notmatch $excluir
        } | Select-Object -First 1
        if ($c) {
            $ip = ($c.IPv4Address | Where-Object { $_.IPAddress -notlike '169.254.*' } | Select-Object -First 1).IPAddress
            if ($ip) { return $ip }
        }
    } catch { }
    try {
        # Laboratorio sin router: tomar el adaptador físico conectado
        $fisicos = @(Get-NetAdapter -Physical -ErrorAction Stop | Where-Object { $_.Status -eq 'Up' })
        foreach ($f in $fisicos) {
            $ip = (Get-NetIPAddress -AddressFamily IPv4 -InterfaceIndex $f.ifIndex -ErrorAction SilentlyContinue |
                Where-Object { $_.PrefixOrigin -in @('Dhcp', 'Manual') -and $_.IPAddress -notlike '169.254.*' } | Select-Object -First 1).IPAddress
            if ($ip) { return $ip }
        }
    } catch { }
    return $null
}

function Get-IpsLocales {
    if (-not $EsWindows) { return @((& hostname -I 2>$null) -split ' ' | Where-Object { $_ }) + @('127.0.0.1') }
    try { return @(Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop | ForEach-Object { $_.IPAddress }) } catch { return @('127.0.0.1') }
}

# IP de esta PC para el .env: si ya figura una de sus IP en el .env (otra PC lo armó), esa; si no, la detectada.
# Con Wi-Fi y cable a la vez, la detección sola podría elegir la red equivocada.
function Get-IpParaEnv($Cfg, [string[]]$Locales) {
    foreach ($ip in @($Cfg['NODE_A_IP'], $Cfg['NODE_B_IP']) + (Get-Generadores $Cfg)) {
        if ($ip -and $ip -notlike '127.*' -and $Locales -contains $ip) { return $ip }
    }
    return Get-IpLan
}

function Test-IpValida([string]$Ip) {
    $x = $null
    return ($Ip -match '^\d{1,3}(\.\d{1,3}){3}$' -and [System.Net.IPAddress]::TryParse($Ip, [ref]$x))
}

function Test-Tcp([string]$Equipo, [int]$Puerto, [int]$Milisegundos = 2000) {
    $cliente = New-Object System.Net.Sockets.TcpClient
    try {
        $intento = $cliente.BeginConnect($Equipo, $Puerto, $null, $null)
        if (-not $intento.AsyncWaitHandle.WaitOne($Milisegundos)) { return $false }
        $cliente.EndConnect($intento)
        return $true
    } catch { return $false } finally { $cliente.Close() }
}

function Get-AutorizacionAdmin($Cfg) {
    $par = '{0}:{1}' -f $Cfg['ADMIN_USER'], $Cfg['ADMIN_PASSWORD']
    return @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($par)) }
}

function Invoke-Api([string]$Metodo, [string]$Url, [hashtable]$Cabeceras = @{}, [int]$Timeout = 20) {
    try {
        return Invoke-RestMethod -Method $Metodo -Uri $Url -Headers $Cabeceras -TimeoutSec $Timeout -UseBasicParsing -ErrorAction Stop
    } catch {
        $detalle = $_.Exception.Message
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $detalle = $_.ErrorDetails.Message }
        throw "No se pudo llamar a $Url : $detalle"
    }
}

function Test-Http([string]$Url) {
    try {
        $r = Invoke-WebRequest -Uri $Url -TimeoutSec 4 -UseBasicParsing -ErrorAction Stop
        return ($r.StatusCode -ge 200 -and $r.StatusCode -lt 400)
    } catch { return $false }
}

# Direcciones de cada servicio según el modo
# Direcciones de cada servicio. En varias PCs, los comandos de demo hablan con el simulador del nodo C
# (o el de esta PC si es generador); "-Nodo D" elige el del nodo D.
function Get-Urls($Cfg) {
    $roles = Get-RolesLocales
    $unico = $roles -contains 'UNICO'
    $ipB = if ($unico -or $roles -contains 'B') { 'localhost' } else { $Cfg['NODE_B_IP'] }
    $gens = @(Get-Generadores $Cfg)
    $ipSim = 'localhost'
    if (-not $unico) {
        $letra = if ($Nodo -match '^\s*[CcDd]\s*$') { $Nodo.Trim().ToUpper() } elseif ($roles -contains 'D') { 'D' } else { 'C' }
        $indice = if ($letra -eq 'D') { 1 } else { 0 }
        if ($roles -contains $letra) { $ipSim = 'localhost' }
        elseif ($gens.Count -gt $indice) { $ipSim = $gens[$indice] }
    }
    return @{
        Simulador  = 'http://{0}:{1}' -f $ipSim, $Cfg['SIMULATOR_PORT']
        Consola    = 'http://{0}:{1}' -f $ipB, $Cfg['ALERT_SERVICE_PORT']
        Grafana    = 'http://{0}:{1}' -f $ipB, $Cfg['GRAFANA_PORT']
        Prometheus = 'http://{0}:{1}' -f $ipB, $Cfg['PROMETHEUS_PORT']
        Ingesta    = 'http://{0}:{1}' -f $ipB, $Cfg['INGESTION_PORT']
        Correlacion = 'http://{0}:{1}' -f $ipB, $Cfg['CORRELATION_PORT']
    }
}

# Simuladores de todos los generadores (C y D). En una sola PC, el local.
function Get-Simuladores($Cfg) {
    $roles = Get-RolesLocales
    if ($roles -contains 'UNICO') { return @(@{ Nodo = 'C'; Url = 'http://localhost:{0}' -f $Cfg['SIMULATOR_PORT'] }) }
    $lista = @()
    $gens = @(Get-Generadores $Cfg)
    for ($i = 0; $i -lt [math]::Min(2, $gens.Count); $i++) {
        $letra = if ($i -eq 0) { 'C' } else { 'D' }
        $ip = if ($roles -contains $letra) { 'localhost' } else { $gens[$i] }
        $lista += @{ Nodo = $letra; Url = 'http://{0}:{1}' -f $ip, $Cfg['SIMULATOR_PORT'] }
    }
    return $lista
}

# Simuladores sobre los que actúa un comando de demo: el de -Nodo C|D, o todos
function Get-SimuladoresObjetivo($Cfg) {
    $todos = @(Get-Simuladores $Cfg)
    if ($Nodo -match '^\s*[CcDd]\s*$') { return @($todos | Where-Object { $_.Nodo -eq $Nodo.Trim().ToUpper() }) }
    return $todos
}

# =============================================================================================
# Puertos: rangos reservados por Windows (Hyper-V/WSL/WinNAT), ocupados, duplicados
# =============================================================================================
function Get-RangosExcluidos {
    if (-not $EsWindows -or -not (Get-Command netsh -ErrorAction SilentlyContinue)) { return @() }
    $salida = & netsh interface ipv4 show excludedportrange protocol=tcp 2>$null
    $rangos = @()
    foreach ($l in $salida) {
        # Solo las líneas numéricas: no depende del idioma de Windows. "*" = exclusión administrada.
        if ($l -match '^\s*(\d+)\s+(\d+)\s*(\*?)\s*$') {
            $rangos += [pscustomobject]@{ Inicio = [int]$Matches[1]; Fin = [int]$Matches[2]; Administrado = ($Matches[3] -eq '*') }
        }
    }
    return $rangos
}

function Test-Bind([int]$Puerto) {
    $s = New-Object System.Net.Sockets.Socket([System.Net.Sockets.AddressFamily]::InterNetwork, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)
    try {
        $s.ExclusiveAddressUse = $true
        $s.Bind((New-Object System.Net.IPEndPoint([System.Net.IPAddress]::Any, $Puerto)))
        return 'libre'
    } catch {
        $e = $_.Exception
        while ($e -and -not ($e -is [System.Net.Sockets.SocketException])) { $e = $e.InnerException }
        if (-not $e) { return 'error' }
        switch ([string]$e.SocketErrorCode) {
            'AccessDenied' { return 'reservado' }
            'AddressAlreadyInUse' { return 'ocupado' }
            default { return 'error' }
        }
    } finally { $s.Close() }
}

function Get-DuenoPuerto([int]$Puerto) {
    if (-not $EsWindows) { return $null }
    try {
        $c = Get-NetTCPConnection -State Listen -LocalPort $Puerto -ErrorAction Stop | Select-Object -First 1
        if ($c) { return (Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue).ProcessName }
    } catch { }
    return $null
}

function Test-Puerto([int]$Puerto, $Rangos, [string[]]$Proyectos = @()) {
    $contenedor = Get-ContenedorConPuerto $Puerto
    if ($contenedor) {
        $nombre = $contenedor.Nombre
        if ($contenedor.Proyecto -and $Proyectos -contains $contenedor.Proyecto) { return @{ Estado = 'centinela'; Detalle = "lo usa $nombre (ya está levantado)" } }
        if (Get-RolDeProyecto $contenedor.Proyecto) {
            return @{ Estado = 'otro-modo'; Proyecto = $contenedor.Proyecto; Detalle = "lo usa $nombre, de otro modo de Centinela que sigue encendido en esta PC" }
        }
        return @{ Estado = 'ocupado'; Detalle = "lo usa el contenedor $nombre" }
    }
    foreach ($r in $Rangos) {
        if (-not $r.Administrado -and $Puerto -ge $r.Inicio -and $Puerto -le $r.Fin) {
            return @{ Estado = 'reservado'; Detalle = "Windows reservó el rango $($r.Inicio)-$($r.Fin) (Hyper-V/WSL)" }
        }
    }
    switch (Test-Bind $Puerto) {
        'libre' { return @{ Estado = 'libre'; Detalle = '' } }
        'reservado' { return @{ Estado = 'reservado'; Detalle = 'Windows no permite usarlo (rango reservado, antivirus o firewall)' } }
        'ocupado' {
            $dueno = Get-DuenoPuerto $Puerto
            $quien = if ($dueno) { "lo usa el programa $dueno" } else { 'lo usa otro programa' }
            return @{ Estado = 'ocupado'; Detalle = $quien }
        }
        default { return @{ Estado = 'error'; Detalle = 'no se pudo comprobar' } }
    }
}

function Get-RevisionPuertos([string[]]$Roles, $Cfg) {
    $rangos = Get-RangosExcluidos
    $proyectos = @($Roles | ForEach-Object { (Get-InfoRol $_ $Cfg).Proyecto })
    $vars = @()
    foreach ($r in $Roles) { foreach ($v in (Get-VariablesPuerto $r $Cfg)) { if ($vars -notcontains $v) { $vars += $v } } }
    $vistos = @{}
    $resultado = @()
    foreach ($v in $vars) {
        $p = Get-Puerto $Cfg $v
        if ($vistos.ContainsKey($p)) {
            $resultado += [pscustomobject]@{ Variable = $v; Puerto = $p; Estado = 'duplicado'; Detalle = "también lo usa $($vistos[$p]) en esta PC" }
            continue
        }
        $vistos[$p] = $v
        $t = Test-Puerto $p $rangos $proyectos
        $resultado += [pscustomobject]@{ Variable = $v; Puerto = $p; Estado = $t.Estado; Detalle = $t.Detalle; Proyecto = $t.Proyecto }
    }
    return $resultado
}

function Find-PuertoLibre([int]$Base, [int[]]$Evitar) {
    # 29092 y 9093 los usa Kafka por dentro (listener interno y controlador): nunca se ofrecen
    $Evitar = @($Evitar) + @(29092, 9093)
    $rangos = Get-RangosExcluidos
    $candidatos = @()
    foreach ($salto in @(10000, 20000, 1000, 30000)) { for ($i = 0; $i -lt 20; $i++) { $candidatos += $Base + $salto + $i } }
    foreach ($c in $candidatos) {
        if ($c -lt 1024 -or $c -ge 49152 -or $Evitar -contains $c) { continue }
        if ((Test-Puerto $c $rangos).Estado -eq 'libre') { return $c }
    }
    return $null
}

function Show-RevisionPuertos($Revision) {
    foreach ($r in $Revision) {
        $texto = '{0,-24} {1,6}' -f $r.Variable, $r.Puerto
        switch ($r.Estado) {
            'libre' { Write-Ok "$texto  libre" }
            'centinela' { Write-Ok "$texto  $($r.Detalle)" }
            default { Write-Fallo "$texto  $($r.Estado): $($r.Detalle)" }
        }
    }
}

# Revisa y, si se pide, corrige los puertos de esta PC. Devuelve $true si quedaron todos usables.
function Repair-Puertos([string[]]$Roles, $Cfg, [bool]$Corregir) {
    $revision = @(Get-RevisionPuertos $Roles $Cfg)
    Show-RevisionPuertos $revision
    # Otro modo de Centinela encendido (p. ej. se pasó de una PC a varias): se detiene, no se cambian puertos
    $otros = @($revision | Where-Object { $_.Estado -eq 'otro-modo' } | ForEach-Object { $_.Proyecto } | Select-Object -Unique)
    if ($otros.Count -gt 0) {
        Write-Aviso "Sigue encendido otro modo de Centinela en esta PC ($($otros -join ', ')) y usa los mismos puertos."
        if (-not (Read-SiNo '¿Detenerlo ahora? (sus datos se conservan)' $true)) { return $false }
        foreach ($p in $otros) { $null = Invoke-Compose (Get-RolDeProyecto $p) $Cfg @('down') }
        $revision = @(Get-RevisionPuertos $Roles $Cfg)
        Show-RevisionPuertos $revision
    }
    $malos = @($revision | Where-Object { $_.Estado -notin @('libre', 'centinela') })
    if ($malos.Count -eq 0) { return $true }
    if (-not $Corregir) {
        if (-not (Read-SiNo 'Hay puertos que Windows no deja usar. ¿Elegir otros libres automáticamente?')) { return $false }
    }
    $cambios = [ordered]@{}
    $usados = @($revision | ForEach-Object { $_.Puerto })
    foreach ($m in $malos) {
        $nuevo = Find-PuertoLibre ([int]$PuertosPorDefecto[$m.Variable]) $usados
        if (-not $nuevo) { Write-Fallo "No encontré un puerto libre para $($m.Variable)"; return $false }
        $cambios[$m.Variable] = $nuevo
        $usados += $nuevo
        $Cfg[$m.Variable] = [string]$nuevo
        Write-Ok "$($m.Variable): $($m.Puerto) -> $nuevo"
    }
    Save-Env $cambios
    if ($Roles -notcontains 'UNICO') {
        Write-Aviso 'IMPORTANTE: cambió un puerto que usan las otras PCs. Copie de nuevo el archivo .env'
        Write-Info  "($ArchivoEnv) a la carpeta del proyecto de las demás PCs y repita ahí 'centinela.cmd configurar'."
        Write-Aviso "Y en ESTA PC ejecute 'centinela.cmd firewall' para abrir el puerto nuevo."
    }
    return $true
}

# =============================================================================================
# Prometheus del nodo B: objetivos de los nodos generadores (no se pueden interpolar en el JSON)
# =============================================================================================
function Write-ObjetivosPrometheus($Cfg) {
    $gens = @(Get-Generadores $Cfg)
    # JSON armado a mano: ConvertTo-Json de PowerShell 5.1 cambia la forma de los arreglos de un elemento
    $grupos = @(@{ Variable = 'SIMULATOR_PORT'; Servicio = 'sensor-simulator' })
    if (Test-Workers $Cfg) {
        $grupos += @{ Variable = 'WORKER_INGESTION_PORT'; Servicio = 'ingestion-service' }
        $grupos += @{ Variable = 'WORKER_CORRELATION_PORT'; Servicio = 'correlation-engine' }
    }
    $bloques = foreach ($g in $grupos) {
        $lista = ($gens | ForEach-Object { '"{0}:{1}"' -f $_, $Cfg[$g.Variable] }) -join ', '
        '  {{ "targets": [{0}], "labels": {{ "servicio": "{1}" }} }}' -f $lista, $g.Servicio
    }
    $ruta = [IO.Path]::Combine($Raiz, 'deploy', 'nodo-b', 'targets', 'generadores.json')
    $json = "[`n" + (@($bloques) -join ",`n") + "`n]`n"
    [IO.File]::WriteAllText($ruta, $json, (New-Object System.Text.UTF8Encoding($false)))
    Write-Ok "Prometheus raspará los generadores: $($gens -join ', ')"
}

# =============================================================================================
# Firewall de Windows (requiere administrador; se pide elevación solo para esto)
# =============================================================================================
function Test-Admin {
    if (-not $EsWindows) { return $false }
    $p = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Invoke-Elevado([string[]]$Argumentos) {
    $exe = (Get-Process -Id $PID).Path
    $lista = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', ('"{0}"' -f $PSCommandPath)) + $Argumentos + @('-Pausa')
    try {
        $p = Start-Process -FilePath $exe -Verb RunAs -ArgumentList ($lista -join ' ') -Wait -PassThru -ErrorAction Stop
        return ($p.ExitCode -eq 0)
    } catch {
        Write-Fallo 'Se canceló la ventana de administrador.'
        return $false
    }
}

function Get-PuertosFirewall([string[]]$Roles, $Cfg) {
    $puertos = @()
    foreach ($r in $Roles) {
        $vars = switch ($r) {
            'UNICO' { @('ALERT_SERVICE_PORT', 'GRAFANA_PORT', 'SIMULATOR_PORT') }   # para ver la consola desde otra PC o un proyector
            default { Get-VariablesPuerto $r $Cfg }
        }
        foreach ($v in $vars) { $puertos += [string](Get-Puerto $Cfg $v) }
    }
    return @($puertos | Select-Object -Unique)
}

function Invoke-ComandoFirewall {
    if (-not $EsWindows) { Write-Aviso 'El firewall solo se configura en Windows.'; return }
    $cfg = Get-Config
    $roles = Get-Roles
    if ($roles.Count -eq 0) { Stop-Centinela "Primero ejecute 'centinela.cmd configurar'." }
    if (-not (Test-Admin)) {
        Write-Info 'Se abrirá una ventana de administrador (Windows pedirá confirmación).'
        $args2 = @('firewall')
        if ($Quitar) { $args2 += '-Quitar' }
        if ($Nodo) { $args2 += @('-Nodo', $Nodo) }
        if (Invoke-Elevado $args2) { Write-Ok 'Firewall configurado.' }
        else { Write-Fallo 'El firewall no quedó configurado (vea el mensaje de la ventana de administrador).' }
        return
    }
    Write-Titulo 'Firewall de Windows'
    Get-NetFirewallRule -Group $GrupoFirewall -ErrorAction SilentlyContinue | Remove-NetFirewallRule -ErrorAction SilentlyContinue
    if ($Quitar) { Write-Ok "Se quitaron las reglas del grupo '$GrupoFirewall'."; return }
    # Si alguien pulsó "Cancelar" en el aviso del firewall, Windows creó reglas que BLOQUEAN a Docker
    # Desktop, y un bloqueo gana sobre cualquier regla de permiso.
    $bloqueos = @(Get-NetFirewallApplicationFilter -ErrorAction SilentlyContinue |
        Where-Object { $_.Program -like '*com.docker.backend.exe' -or $_.Program -like '*vpnkit.exe' } |
        Get-NetFirewallRule -ErrorAction SilentlyContinue | Where-Object { $_.Direction -eq 'Inbound' -and $_.Action -eq 'Block' })
    if ($bloqueos.Count -gt 0) {
        $bloqueos | Remove-NetFirewallRule
        Write-Ok "Se quitaron $($bloqueos.Count) reglas que bloqueaban a Docker Desktop."
    }
    $puertos = Get-PuertosFirewall $roles $cfg
    # La subred local y, en varias PCs, las IP de los nodos: una PC por Wi-Fi o en otra sala queda en otra subred
    $remotos = @('LocalSubnet')
    if ($roles -notcontains 'UNICO') {
        $remotos += @(@($cfg['NODE_A_IP'], $cfg['NODE_B_IP']) + (Get-Generadores $cfg) | Where-Object { $_ -and (Test-IpValida $_) } | Select-Object -Unique)
    }
    try {
        New-NetFirewallRule -DisplayName ("Centinela Urbano - {0} (TCP {1})" -f ($roles -join ','), ($puertos -join ',')) -Group $GrupoFirewall `
            -Direction Inbound -Action Allow -Protocol TCP -LocalPort $puertos -Profile Any -RemoteAddress $remotos -ErrorAction Stop | Out-Null
    } catch {
        Stop-Centinela "Windows no aceptó la regla del firewall: $($_.Exception.Message)"
    }
    Write-Ok "Entrada permitida a los puertos TCP $($puertos -join ', ') desde: $($remotos -join ', ')."
    Write-Info "Para quitarlo: centinela.cmd firewall -Quitar"
}

# =============================================================================================
# Comandos
# =============================================================================================
function Show-Ayuda {
    Write-Host @'

  Centinela Urbano - operación desde Windows (solo necesita Docker Desktop)

  PREPARAR
    configurar [-Nodo UNICO|A|B|C|D] [-IpA ip] [-IpB ip] [-Generadores ip1,ip2] [-Workers] [-Si]
                         Crea .env, detecta la IP, reparte zonas y revisa los puertos de esta PC.
    requisitos           Docker Desktop, memoria, OneDrive, puertos.
    puertos [-Arreglar]  Revisa los puertos; con -Arreglar elige otros libres y los guarda en .env.
    firewall [-Quitar]   Abre (o cierra) en el firewall los puertos de este nodo. Pide administrador.
    compilar             Compila los jar con Maven dentro de Docker (sin instalar Java) y usa
                         BUILD_TARGET=prebuilt para crear las imágenes en segundos.
    pruebas [modulo]     Corre las pruebas automatizadas dentro de Docker (p. ej. pruebas correlation-engine).

  OPERAR
    iniciar [-Compilar]  Levanta lo que corre en esta PC (en varias PCs: primero A, luego B, luego C/D).
                         Con -Nodo A solo ese nodo, si esta PC tiene varios.
    detener [-Borrar]    Detiene esta PC. -Borrar también elimina los datos (pide confirmación).
    estado               Contenedores, salud de los nodos y conectividad entre PCs.
    logs [servicio]      Últimos registros (p. ej. logs alert-service).
    abrir                Abre la consola del operador, Grafana y el vigilante en el navegador.

  DEMO
    escenarios           Lista los 10 escenarios.
    escenario <id> [-Zona 4] [-Velocidad 1]   Lanza uno (p. ej. escenario tabla-2-4), aislado del ruido de fondo.
                         -Nodo D usa el simulador de D.
    validar              Validación automática de las 9 correlaciones esperadas (~2,5 min).
    reporte "texto" [-Zona 4] [-Tipo ROBO]    Reporte ciudadano con texto libre, por la puerta HTTP de la app.
                         Tipos: ROBO, ACCIDENTE, INCENDIO, ALTERCADO, PERSONA_SOSPECHOSA, OTRO.
    panico [-Zona 10] [-Tipo PANICO]          Botón de pánico desde la app (PANICO, ROBO, ASALTO, EMERGENCIA_MEDICA...).
    pico [-Multiplicador 20] [-Segundos 60]   Pico de carga en todos los generadores (-Nodo C|D: solo uno).
    fondo on|off         Enciende o apaga el tráfico de fondo de los generadores (-Nodo C|D: solo uno).
    replay [archivo.csv] [-Velocidad 2]       Reproduce un CSV con aleatorización.
    caida <A|B|C>        Simula la caída de un nodo (apaga sus contenedores en ESTA PC).
    recuperar <A|B|C>    Vuelve a encender ese nodo.
    particiones          Reparto de particiones de Kafka entre las instancias (balanceo).
    dlq                  Últimos mensajes de la cola de errores (DLQ).
    reprocesar-dlq [-IncluirPermanentes]   Reinyecta la DLQ (por defecto, lo que falló por errores transitorios).
    tarea <nombre>       Ejecuta una tarea periódica ya (rule-quality, slo, plate-retention, alert-expiry, watchlist).
    reiniciar-correlacion  Limpia el estado temporal del motor (pide confirmación).

  DATOS
    respaldo             Respaldo inmediato de la base (además del automático cada 6 h).
    restaurar <archivo>  Restaura un respaldo de la carpeta backups\ (pide confirmación).
    exportar-imagenes [archivo.tar]   Guarda las imágenes para llevarlas en USB (laboratorio sin internet).
    importar-imagenes [archivo.tar]   Carga esas imágenes en esta PC.

  Ejemplos:  centinela.cmd configurar
             centinela.cmd iniciar
             centinela.cmd escenario tabla-2-4
'@
}

function Invoke-Configurar {
    Write-Titulo 'Configuración de Centinela Urbano'
    $existe = Test-Path -LiteralPath $ArchivoEnv
    $cfg = Get-Config
    $previo = Read-Env $ArchivoEnv          # lo que ya decía el .env (para avisar si cambia algo compartido)
    $rolesPrevios = @(Get-RolesLocales)
    $ipsLocales = @(Get-IpsLocales | Where-Object { $_ -notlike '127.*' -and $_ -notlike '169.254.*' })
    $ipLocal = Get-IpParaEnv $cfg $ipsLocales

    # 1. Modo y nodos de esta PC
    $roles = @()
    if ($Nodo) { $roles = ConvertTo-Roles $Nodo }
    else {
        Write-Info '1) Una sola PC (todo junto: recomendado para probar y como plan B)'
        Write-Info '2) Varias PCs (nodos A, B, C/D repartidos en la red local)'
        $modo = Read-Valor 'Elija 1 o 2' '1'
        if ($modo -eq '1') { $roles = @('UNICO') }
        else {
            $sugerido = ''
            if ($existe -and $ipLocal) {
                $s = @()
                if ($cfg['NODE_A_IP'] -eq $ipLocal) { $s += 'A' }
                if ($cfg['NODE_B_IP'] -eq $ipLocal) { $s += 'B' }
                $g = @(Get-Generadores $cfg)
                $idx = [array]::IndexOf([string[]]$g, [string]$ipLocal)
                if ($idx -eq 0) { $s += 'C' } elseif ($idx -eq 1) { $s += 'D' }
                $sugerido = $s -join ','
            }
            if (-not $sugerido) { $sugerido = 'A' }
            $roles = ConvertTo-Roles (Read-Valor '¿Qué nodo(s) corre ESTA PC? (A=datos, B=servicios, C/D=generadores; p. ej. A o A,B)' $sugerido)
        }
    }
    $unico = $roles -contains 'UNICO'
    $cambios = [ordered]@{}

    # 2. Varias PCs: todas deben usar el MISMO .env
    if (-not $unico) {
        # Un .env con la llave de ejemplo (p. ej. creado por 'compilar') no es el de la primera PC
        if (-not $existe -or $cfg['PLATE_HMAC_SECRET'] -like 'cambie-esta-llave*') {
            Write-Aviso 'En varias PCs todas usan el mismo archivo .env (IPs, puertos y secretos iguales).'
            if (-not (Read-SiNo '¿Es esta la PRIMERA PC que configura?' $true)) {
                Stop-Centinela "Copie el .env de la primera PC a $ArchivoEnv y vuelva a ejecutar 'centinela.cmd configurar'."
            }
        } elseif ($rolesPrevios -contains 'UNICO' -or $cfg['NODE_A_IP'] -eq '192.168.1.10') {
            # Una PC que se probó en modo de una sola PC tiene SU propio .env (su propia llave de seudonimización):
            # con otra llave, sus workers no reconocerían las placas que seudonimiza el resto
            Write-Aviso 'Este .env se creó en esta PC (modo de una sola PC). En varias PCs todas deben usar el .env de la primera PC.'
            if (-not (Read-SiNo '¿Este .env ya es el de la primera PC (o esta es la primera PC)?' $true)) {
                Stop-Centinela "Copie el .env de la primera PC a $ArchivoEnv y vuelva a ejecutar 'centinela.cmd configurar'."
            }
        }
        if ($ipLocal) { Write-Ok "IP de esta PC: $ipLocal" } else { Write-Aviso 'No pude detectar la IP de esta PC (ejecute ipconfig).' }
        $defA = if ($IpA) { $IpA } elseif ($roles -contains 'A' -and $ipLocal) { $ipLocal } else { $cfg['NODE_A_IP'] }
        $defB = if ($IpB) { $IpB } elseif ($roles -contains 'B' -and $ipLocal) { $ipLocal } else { $cfg['NODE_B_IP'] }
        $a = Read-Valor 'IP del nodo A (datos: Kafka, TimescaleDB, Redis)' $defA
        $b = Read-Valor 'IP del nodo B (servicios, consola, Grafana)' $defB
        $gens = @(if ($Generadores) { $Generadores -split '[,; ]+' | Where-Object { $_ } } else { Get-Generadores $cfg })
        foreach ($r in @('C', 'D')) {
            if ($roles -contains $r -and $ipLocal) {
                $i = if ($r -eq 'C') { 0 } else { 1 }
                $lista = New-Object System.Collections.Generic.List[string]
                foreach ($x in $gens) { $lista.Add($x) }
                while ($lista.Count -le $i) { $lista.Add($ipLocal) }
                $lista[$i] = $ipLocal
                $gens = @($lista)
            }
        }
        $g = Read-Valor 'IP del nodo C (y del nodo D si existe, separadas por coma)' ($gens -join ',')
        $nGen = @($g -split '[,; ]+' | Where-Object { $_ }).Count
        if ($nGen -gt 2) { Stop-Centinela 'Hay como máximo dos nodos generadores (C y D): indique una o dos IP.' }
        if ($nGen -eq 2) { Write-Info 'Con dos generadores, C simula las zonas 1-6 y D las 7-12: levante también el nodo D.' }
        foreach ($ip in @($a, $b) + @($g -split '[,; ]+' | Where-Object { $_ })) {
            if (-not (Test-IpValida $ip)) { Stop-Centinela "IP inválida: $ip" }
            if ($ip -like '127.*' -or $ip -like '169.254.*') { Stop-Centinela "La IP $ip no sirve entre PCs: use la IP de la red local (ipconfig)." }
        }
        # El nodo que corre aquí debe anunciarse con una IP de esta PC (las demás se conectan a ella)
        $propias = @()
        if ($roles -contains 'A') { $propias += ,@('A', $a) }
        if ($roles -contains 'B') { $propias += ,@('B', $b) }
        $listaGen = @($g -split '[,; ]+' | Where-Object { $_ })
        if ($roles -contains 'C' -and $listaGen.Count -ge 1) { $propias += ,@('C', $listaGen[0]) }
        if ($roles -contains 'D' -and $listaGen.Count -ge 2) { $propias += ,@('D', $listaGen[1]) }
        foreach ($p in $propias) {
            if ($ipsLocales.Count -gt 0 -and $ipsLocales -notcontains $p[1]) {
                Write-Aviso "La IP del nodo $($p[0]) ($($p[1])) no es de esta PC. IPs de esta PC: $($ipsLocales -join ', ')."
                if (-not (Read-SiNo '¿Continuar igual?' $false)) { Stop-Centinela "Repita 'centinela.cmd configurar' con la IP correcta." }
            }
        }
        $w = if ($Workers) { $true } elseif ($SinWorkers) { $false } elseif ($Si) { Test-Workers $cfg } else { Read-SiNo '¿Levantar también workers de ingesta y correlación en C/D (reparto de carga)?' (Test-Workers $cfg) }
        $cambios['NODE_A_IP'] = $a; $cambios['NODE_B_IP'] = $b; $cambios['GENERATOR_IPS'] = $g
        $cambios['WORKERS'] = if ($w) { 'true' } else { 'false' }
        foreach ($k in $cambios.Keys) { $cfg[$k] = $cambios[$k] }
        # B y los workers en la misma PC no pueden publicar los mismos puertos
        if ($w -and ($roles -contains 'B') -and (($roles -contains 'C') -or ($roles -contains 'D'))) {
            if ($cfg['WORKER_INGESTION_PORT'] -eq $cfg['INGESTION_PORT']) { $cambios['WORKER_INGESTION_PORT'] = '9081'; $cfg['WORKER_INGESTION_PORT'] = '9081' }
            if ($cfg['WORKER_CORRELATION_PORT'] -eq $cfg['CORRELATION_PORT']) { $cambios['WORKER_CORRELATION_PORT'] = '9082'; $cfg['WORKER_CORRELATION_PORT'] = '9082' }
        }
    }

    # 3. Secreto de seudonimización: uno nuevo y aleatorio en la primera configuración
    if (-not $existe -or $cfg['PLATE_HMAC_SECRET'] -like 'cambie-esta-llave*') {
        $bytes = New-Object byte[] 36
        [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        $cambios['PLATE_HMAC_SECRET'] = ([Convert]::ToBase64String($bytes) -replace '[+/=]', 'x')
    }
    # Si esta PC corría otro modo (p. ej. una sola PC y ahora el nodo A), sus contenedores siguen encendidos
    # con los mismos puertos y volverían al reiniciar: se ofrece detenerlos (los datos se conservan)
    $sobran = @($rolesPrevios | Where-Object { $roles -notcontains $_ })
    if ($sobran.Count -gt 0 -and (Test-DockerCorriendo)) {
        $encendidos = @($sobran | Where-Object { @(Get-ComposeServicios $_ $cfg | Where-Object { $_.State -eq 'running' }).Count -gt 0 })
        if ($encendidos.Count -gt 0 -and (Read-SiNo "Esta PC tenía configurado el nodo/modo $($encendidos -join ', ') y sigue encendido. ¿Detenerlo? (sus datos se conservan)" $true)) {
            foreach ($r in $encendidos) { $null = Invoke-Compose $r $cfg @('down') }
        }
    }
    Save-Env $cambios
    Save-Local $roles
    Write-Ok "Configuración guardada en $ArchivoEnv"

    # 4. Puertos
    Write-Titulo 'Puertos de esta PC'
    $cfg = Get-Config
    $null = Repair-Puertos $roles $cfg ([bool]$Arreglar)
    $cfg = Get-Config

    # 5. Nodo B: objetivos de Prometheus
    if ($roles -contains 'B') { Write-ObjetivosPrometheus $cfg }

    # 6. Firewall (solo varias PCs)
    if (-not $unico -and $EsWindows) {
        Write-Titulo 'Firewall'
        if (Read-SiNo '¿Abrir en el firewall los puertos de este nodo hacia la red local? (pide administrador)' $true) { Invoke-ComandoFirewall }
    }

    Write-Titulo 'Listo'
    if ($unico) { Write-Info 'Siguiente paso: centinela.cmd iniciar' }
    else {
        Write-Info "Esta PC: nodo(s) $($roles -join ', ')."
        # Valores que comparten todas las PCs: si cambiaron aquí, las demás deben recibir este .env
        $compartidas = @('NODE_A_IP', 'NODE_B_IP', 'GENERATOR_IPS', 'WORKERS', 'WORKER_INGESTION_PORT', 'WORKER_CORRELATION_PORT')
        $cambiadas = @($compartidas | Where-Object { $cambios.Contains($_) -and [string]$previo[$_] -ne [string]$cambios[$_] })
        if (-not $existe) { Write-Aviso "Copie el archivo .env a la carpeta del proyecto de las otras PCs y ejecute ahí 'centinela.cmd configurar'." }
        elseif ($cambiadas.Count -gt 0) {
            Write-Aviso "Cambiaron valores que usan todas las PCs ($($cambiadas -join ', '))."
            Write-Info  "Copie este .env ($ArchivoEnv) a las demás PCs y ejecute ahí 'centinela.cmd configurar'"
            Write-Info  '(en el nodo B eso también actualiza qué generadores mide Prometheus).'
        }
        Write-Info 'Orden de arranque: nodo A -> nodo B -> nodos C/D, con "centinela.cmd iniciar" en cada PC.'
    }
}

function Assert-Configurado {
    if (-not (Test-Path -LiteralPath $ArchivoEnv) -or (Get-Roles).Count -eq 0) {
        Write-Aviso 'Esta PC todavía no está configurada.'
        Invoke-Configurar
    }
}

function Invoke-Requisitos {
    Write-Titulo 'Requisitos'
    if ($EsWindows) {
        $os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
        if ($os) { Write-Ok "$($os.Caption) (compilación $($os.BuildNumber))" }
        if ($os -and [int]$os.BuildNumber -lt 19045) { Write-Aviso 'Docker Desktop pide Windows 10 22H2 (19045) o Windows 11.' }
    }
    Assert-Docker
    if ($EsWindows) {
        # Los archivos sin extensión de la carpeta de Docker son suyos (para Git Bash/WSL): solo se avisa de los de otras carpetas
        $dirDocker = if ($DockerExe) { Split-Path -Parent $DockerExe } else { '' }
        $ajenos = @(Get-Command -Name 'docker' -All -ErrorAction SilentlyContinue | Where-Object {
                $_.Source -and -not [IO.Path]::GetExtension($_.Source) -and (Split-Path -Parent $_.Source) -ne $dirDocker -and
                $_.Source -notlike (Join-Path $env:ProgramFiles 'Docker\*') })
        foreach ($f in $ajenos) {
            Write-Aviso "Hay un archivo 'docker' sin extensión en $($f.Source): no viene de Docker y hace que Windows pregunte con qué abrirlo."
            Write-Info  '       Este script ya lo evita (usa docker.exe), pero conviene renombrarlo o borrarlo (requiere administrador).'
        }
    }
    $tipo = & $DockerExe info --format '{{.OSType}}' 2>$null
    if ($tipo -eq 'linux') { Write-Ok 'Docker Desktop en modo de contenedores Linux' }
    else { Write-Fallo 'Docker Desktop está en modo de contenedores Windows: clic derecho en el ícono > "Switch to Linux containers".' }
    $vc = (& $DockerExe compose version --short 2>$null) -replace '^v', ''
    if ($vc -and [int]($vc.Split('.')[0]) -ge 2) { Write-Ok "Docker Compose $vc" } else { Write-Fallo 'Falta Docker Compose v2 (viene con Docker Desktop actualizado).' }
    $memBytes = & $DockerExe info --format '{{.MemTotal}}' 2>$null
    $memGb = [math]::Round(([double]$memBytes) / 1GB, 1)
    $roles = Get-Roles
    $necesaria = 0
    foreach ($r in $roles) { $necesaria += $MemoriaPorRol[$r] }
    if ($necesaria -eq 0) { $necesaria = $MemoriaPorRol['UNICO'] }
    if ($memGb -ge $necesaria) { Write-Ok "Memoria para Docker: $memGb GB (se recomiendan $necesaria GB)" }
    else {
        Write-Aviso "Docker tiene $memGb GB y se recomiendan $necesaria GB para esta PC."
        Write-Info  'Con WSL 2 se ajusta en %UserProfile%\.wslconfig ([wsl2] memory=8GB), luego "wsl --shutdown" y reabrir Docker Desktop.'
    }
    Show-Relojes (Get-Config)
    if ($Raiz -match 'OneDrive') {
        Write-Aviso 'El proyecto está dentro de OneDrive. Funciona, pero conviene clonarlo fuera (p. ej. C:\dev\Arqui2)'
        Write-Info  'o marcar la carpeta como "Mantener siempre en este dispositivo" para que Docker no lea archivos solo en línea.'
    }
    if ((Test-Path -LiteralPath $ArchivoEnv) -and $roles.Count -gt 0) {
        Write-Titulo 'Puertos'
        $null = Repair-Puertos $roles (Get-Config) ([bool]$Arreglar)
    } else { Write-Info "Ejecute 'centinela.cmd configurar' para revisar también los puertos." }
}

function Invoke-Puertos {
    Assert-Configurado
    Write-Titulo 'Puertos de esta PC'
    $ok = Repair-Puertos (Get-Roles) (Get-Config) ([bool]$Arreglar)
    if ($EsWindows) {
        $dinamico = (& netsh int ipv4 show dynamicport tcp 2>$null) -join ' '
        if ($dinamico -match '(\d{4,5})\D+(\d{3,5})') {
            $inicio = [int]$Matches[1]
            if ($inicio -lt 49152) {
                Write-Aviso "El rango dinámico de Windows empieza en $inicio (lo normal es 49152): por eso Hyper-V/WSL reservan puertos bajos."
                Write-Info  'Arreglo permanente (cmd como administrador y reiniciar):'
                Write-Info  '  netsh int ipv4 set dynamic tcp start=49152 num=16384'
                Write-Info  '  netsh int ipv6 set dynamic tcp start=49152 num=16384'
            }
        }
    }
    if (-not $ok) {
        Write-Info 'Opciones: "centinela.cmd puertos -Arreglar" (sin administrador) o, como administrador:'
        Write-Info '  net stop winnat  y luego  net start winnat   (libera las reservas hasta el próximo reinicio)'
        exit 1
    }
}

function Wait-Saludable([string]$Rol, $Cfg) {
    $limite = (Get-Date).AddMinutes(5)
    $urls = switch ($Rol) {
        'UNICO' { @(('http://localhost:{0}/actuator/health' -f $Cfg['ALERT_SERVICE_PORT']), ('http://localhost:{0}/actuator/health' -f $Cfg['SIMULATOR_PORT'])) }
        'B' { @(('http://localhost:{0}/actuator/health' -f $Cfg['ALERT_SERVICE_PORT']), ('http://localhost:{0}/actuator/health' -f $Cfg['INGESTION_PORT']), ('http://localhost:{0}/actuator/health' -f $Cfg['CORRELATION_PORT'])) }
        'A' { @() }
        default { @(('http://localhost:{0}/actuator/health' -f $Cfg['SIMULATOR_PORT'])) }
    }
    Write-Host "       Esperando a que el nodo $Rol esté sano" -NoNewline
    while ((Get-Date) -lt $limite) {
        $listo = $true
        if ($Rol -eq 'A') {
            $servicios = Get-ComposeServicios 'A' $Cfg
            foreach ($n in @('kafka', 'timescaledb', 'redis')) {
                $s = $servicios | Where-Object { $_.Service -eq $n } | Select-Object -First 1
                if (-not $s -or $s.Health -ne 'healthy') { $listo = $false }
            }
        } else {
            foreach ($u in $urls) { if (-not (Test-Http $u)) { $listo = $false } }
        }
        if ($listo) { Write-Host ''; Write-Ok "Nodo $Rol sano"; return $true }
        Write-Host '.' -NoNewline
        Start-Sleep -Seconds 5
    }
    Write-Host ''
    Write-Aviso "El nodo $Rol no quedó sano en 5 minutos: revise 'centinela.cmd estado' y 'centinela.cmd logs'."
    return $false
}

# Módulos con imagen propia que necesita cada rol
function Get-ModulosPropios([string[]]$Roles, $Cfg) {
    $modulos = @()
    foreach ($r in $Roles) {
        switch ($r) {
            'UNICO' { $modulos += @('sensor-simulator', 'ingestion-service', 'correlation-engine', 'alert-service') }
            'B' { $modulos += @('ingestion-service', 'correlation-engine', 'alert-service') }
            { $_ -in @('C', 'D') } {
                $modulos += 'sensor-simulator'
                if (Test-Workers $Cfg) { $modulos += @('ingestion-service', 'correlation-engine') }
            }
        }
    }
    return @($modulos | Select-Object -Unique)
}

# Módulos que habría que construir con BUILD_TARGET=prebuilt sin tener su jar compilado en esta PC
function Get-JarsFaltantes([string[]]$Roles, $Cfg) {
    return @(Get-ModulosPropios $Roles $Cfg | Where-Object {
            $imagen = & $DockerExe images -q "centinela/${_}:1.0.0" 2>$null
            -not $imagen -and -not (Test-Path -LiteralPath ([IO.Path]::Combine($Raiz, $_, 'target', 'app.jar'))) })
}

function Invoke-Iniciar {
    Assert-Docker
    Assert-Configurado
    $cfg = Get-Config
    $roles = Get-Roles
    Write-Titulo "Iniciando: $($roles -join ', ')"
    if (-not (Repair-Puertos $roles $cfg ([bool]$Arreglar))) { Stop-Centinela 'Hay puertos sin resolver. Use "centinela.cmd puertos -Arreglar".' }
    $cfg = Get-Config
    # Con BUILD_TARGET=prebuilt las imágenes copian los jar ya compilados: -Compilar los recompila antes
    if ($Compilar -and ([string]$cfg['BUILD_TARGET']).Trim() -eq 'prebuilt' -and @($roles | Where-Object { $_ -ne 'A' }).Count -gt 0) {
        Invoke-CompilarJar
        $cfg = Get-Config
    }
    # El .env de la PC que compiló dice BUILD_TARGET=prebuilt, pero en esta PC (un clon nuevo) no están los jar
    $targetPrevio = $env:BUILD_TARGET
    if (-not $Compilar -and ([string]$cfg['BUILD_TARGET']).Trim() -eq 'prebuilt') {
        $faltan = @(Get-JarsFaltantes $roles $cfg)
        if ($faltan.Count -gt 0) {
            Write-Aviso "En esta PC no están compilados: $($faltan -join ', ')."
            Write-Info  'Se construyen con Maven dentro de Docker (la primera vez tarda unos minutos y necesita internet).'
            $env:BUILD_TARGET = 'runtime'   # las variables del proceso tienen prioridad sobre el .env
        }
    }
    try {
        Start-Nodos $roles $cfg
    } finally {
        $env:BUILD_TARGET = $targetPrevio
    }
    Show-Urls $cfg
}

function Start-Nodos([string[]]$Roles, $Cfg) {
    $roles = $Roles
    $cfg = $Cfg
    foreach ($rol in $roles) {
        if ($rol -in @('B', 'C', 'D') -and $roles -notcontains 'A') {
            $ipA = $cfg['NODE_A_IP']
            Write-Host "       Comprobando el nodo A ($ipA)" -NoNewline
            $ok = $false
            for ($i = 0; $i -lt 24 -and -not $ok; $i++) {
                $ok = Test-Tcp $ipA (Get-Puerto $cfg 'KAFKA_PORT')
                if (-not $ok) { Write-Host '.' -NoNewline; Start-Sleep -Seconds 5 }
            }
            Write-Host ''
            if ($ok) { Write-Ok "Kafka del nodo A responde en ${ipA}:$($cfg['KAFKA_PORT'])" }
            else { Write-Aviso "No llego a Kafka en ${ipA}:$($cfg['KAFKA_PORT']). ¿Está iniciado el nodo A y abierto su firewall? Sigo igual: los servicios reintentan solos." }
        }
        if ($rol -eq 'B') { Write-ObjetivosPrometheus $cfg }
        $argumentos = @('up', '-d', '--remove-orphans')
        if ($Compilar) { $argumentos += '--build' }
        $codigo = Invoke-Compose $rol $cfg $argumentos
        if ($codigo -ne 0) {
            Write-Fallo "docker compose terminó con error en el nodo $rol."
            Write-Info  'Si el error dice "ports are not available" o "forbidden by its access permissions": centinela.cmd puertos -Arreglar'
            exit 1
        }
        $null = Wait-Saludable $rol $cfg
    }
}

function Show-Urls($Cfg) {
    $u = Get-Urls $Cfg
    Write-Titulo 'Direcciones'
    Write-Info "Consola del operador:  $($u.Consola)   ($($Cfg['OPERATOR_USER']) / $($Cfg['OPERATOR_PASSWORD']))"
    Write-Info "Grafana:               $($u.Grafana)   (admin / $($Cfg['GRAFANA_ADMIN_PASSWORD']))"
    Write-Info "Simulador y vigilante: $($u.Simulador)/vigilante.html"
    Write-Info "Prometheus:            $($u.Prometheus)/targets"
}

function Invoke-Detener {
    Assert-Docker
    $cfg = Get-Config
    $roles = @(Get-Roles)
    [array]::Reverse($roles)
    if ($Borrar -and -not (Read-SiNo 'Esto BORRA los datos (Kafka, base de datos, spool). ¿Seguro?' $false)) { return }
    foreach ($rol in $roles) {
        Write-Titulo "Deteniendo nodo $rol"
        $argumentos = @('down')
        if ($Borrar) { $argumentos += '-v' }
        if ((Invoke-Compose $rol $cfg $argumentos) -ne 0) { Write-Fallo "docker compose down falló en el nodo $rol (ver arriba)." }
    }
}

function Invoke-Estado {
    Assert-Docker
    $cfg = Get-Config
    $roles = Get-Roles
    foreach ($rol in $roles) {
        Write-Titulo "Contenedores del nodo $rol (esta PC)"
        $null = Invoke-Compose $rol $cfg @('ps', '--format', 'table {{.Service}}\t{{.Status}}\t{{.Ports}}')
    }
    $unico = $roles -contains 'UNICO'
    $u = Get-Urls $cfg
    if (-not $unico) {
        Write-Titulo 'Configuración compartida (.env)'
        Write-Info ("Nodo A {0} | Nodo B {1} | Generadores {2} | Workers {3}" -f $cfg['NODE_A_IP'], $cfg['NODE_B_IP'], $cfg['GENERATOR_IPS'], $cfg['WORKERS'])
        Write-Info ("Huella {0}: debe ser la MISMA en todas las PCs; si difiere, copie el .env de la primera PC y repita 'configurar'" -f (Get-HuellaConfig $cfg))
    }
    Write-Titulo 'Salud'
    $ipA = if ($unico) { 'localhost' } else { $cfg['NODE_A_IP'] }
    $kafkaPort = if ($unico) { Get-Puerto $cfg 'KAFKA_EXTERNAL_PORT' } else { Get-Puerto $cfg 'KAFKA_PORT' }
    $chequeos = @(
        @('Nodo A - Kafka', { Test-Tcp $ipA $kafkaPort }),
        @('Nodo A - TimescaleDB', { Test-Tcp $ipA (Get-Puerto $cfg 'DB_PORT') }),
        @('Nodo A - Redis', { Test-Tcp $ipA (Get-Puerto $cfg 'REDIS_PORT') }),
        @('Nodo B - ingestion-service', { Test-Http "$($u.Ingesta)/actuator/health/liveness" }),
        @('Nodo B - correlation-engine', { Test-Http "$($u.Correlacion)/actuator/health/liveness" }),
        @('Nodo B - alert-service y consola', { Test-Http "$($u.Consola)/actuator/health/liveness" }),
        @('Nodo B - Grafana', { Test-Http "$($u.Grafana)/api/health" }),
        @('Nodo B - Prometheus', { Test-Http "$($u.Prometheus)/-/ready" })
    )
    foreach ($c in $chequeos) { if (& $c[1]) { Write-Ok $c[0] } else { Write-Fallo $c[0] } }
    $gens = @(if ($unico) { 'localhost' } else { Get-Generadores $cfg })
    $i = 0
    foreach ($g in $gens) {
        $letra = [char](67 + $i); $i++
        $ip = if (($roles -contains [string]$letra) -or $unico) { 'localhost' } else { $g }
        try {
            $st = Invoke-Api 'GET' ('http://{0}:{1}/api/simulator/status' -f $ip, $cfg['SIMULATOR_PORT']) @{} 4
            Write-Ok ("Nodo {0} - simulador ({1}): zonas {2}, {3} ev/s, pendientes en spool: {4}" -f $letra, $g, ($st.zones -join ','), $st.expectedEventsPerSecond, $st.spoolPending)
        } catch { Write-Fallo "Nodo $letra - simulador ($g)" }
        try {
            $v = Invoke-Api 'GET' ('http://{0}:{1}/api/watchdog' -f $ip, $cfg['SIMULATOR_PORT']) @{} 4
            $nb = $v.watchdog.nodeB
            $texto = "Vigilante del nodo ${letra}: nodo B $($nb.state). $($nb.summary)"
            if ($nb.state -eq 'OPERATIVO') { Write-Ok $texto } elseif ($nb.state -eq 'DESCONOCIDO') { Write-Info $texto } else { Write-Fallo $texto }
            foreach ($g in @($v.watchdog.kafka.groups)) {
                if ($g.condition -ne 'OK' -and $g.condition -ne 'DESCONOCIDO') { Write-Aviso ("Grupo {0}: {1}" -f $g.group, $g.message) }
            }
        } catch { }
    }
    Write-Titulo 'Relojes'
    Show-Relojes $cfg
}

function Invoke-Logs {
    Assert-Docker
    $cfg = Get-Config
    $roles = Get-Roles
    $argumentos = @('logs', '--tail', '200', '-f')
    if ($Objetivo) { $argumentos += $Objetivo }
    foreach ($rol in $roles) {
        $servicios = @(Get-ComposeServicios $rol $cfg | ForEach-Object { $_.Service })
        if (-not $Objetivo -or $servicios -contains $Objetivo) { $null = Invoke-Compose $rol $cfg $argumentos; return }
    }
    Write-Fallo "El servicio '$Objetivo' no corre en esta PC."
}

function Invoke-Abrir {
    $u = Get-Urls (Get-Config)
    foreach ($url in @($u.Consola, $u.Grafana, "$($u.Simulador)/vigilante.html")) { Start-Process $url }
}

function Invoke-Escenarios {
    $u = Get-Urls (Get-Config)
    $lista = Invoke-Api 'GET' "$($u.Simulador)/api/scenarios"
    Write-Titulo 'Escenarios'
    foreach ($e in @($lista)) { Write-Info ('{0,-20} {1}' -f $e.id, $e.title) }
}

function Invoke-Escenario {
    if (-not $Objetivo) { Stop-Centinela 'Indique el escenario, p. ej.: centinela.cmd escenario tabla-2-4  (vea "centinela.cmd escenarios")' }
    $cfg = Get-Config
    $u = Get-Urls $cfg
    $url = '{0}/api/scenarios/{1}?speed={2}' -f $u.Simulador, $Objetivo, (Get-Velocidad)
    if ($Zona -gt 0) { $url += "&zone=$Zona" }
    $r = Invoke-Api 'POST' $url
    Write-Ok "Escenario '$($r.title)' lanzado. Mírelo en la consola: $($u.Consola)"
    if ($r.isolation) { Write-Info $r.isolation }
    # Con dos generadores, el otro tampoco debe meter ruido en las zonas del escenario
    $zonas = @($r.quietZones) -join ','
    if ($zonas -and $r.quietSeconds) {
        $segundos = [int]$r.quietSeconds
        foreach ($s in @(Get-Simuladores $cfg | Where-Object { $_.Url -ne $u.Simulador })) {
            try {
                $null = Invoke-Api 'POST' ('{0}/api/simulator/quiet?zones={1}&seconds={2}' -f $s.Url, $zonas, [math]::Max(1, [math]::Min(3600, $segundos))) @{} 5
                Write-Info "El simulador del nodo $($s.Nodo) tampoco mete ruido en esas zonas."
            } catch { Write-Aviso "No pude avisar al simulador del nodo $($s.Nodo): su tráfico de fondo puede mezclarse con el escenario." }
        }
    }
    foreach ($e in @($r.expected)) {
        Write-Info ('Se espera {0} en la Zona {1} (severidad mínima {2}) en menos de {3} s.' -f $e.alertType, $e.zoneId, $e.minSeverity, $e.withinSeconds)
    }
}

function Invoke-Validar {
    $cfg = Get-Config
    $u = Get-Urls $cfg
    # La validación corre en un simulador; si hay otro (nodo D), que no meta ruido mientras tanto (~8 min)
    foreach ($s in @(Get-Simuladores $cfg | Where-Object { $_.Url -ne $u.Simulador })) {
        try { $null = Invoke-Api 'POST' ('{0}/api/simulator/quiet?zones=1,2,3,4,5,6,7,8,9,10,11,12&seconds=480' -f $s.Url) @{} 5 }
        catch { Write-Aviso "No pude avisar al simulador del nodo $($s.Nodo): su tráfico de fondo puede mezclarse con la validación." }
    }
    $r = Invoke-Api 'POST' "$($u.Simulador)/api/validation"
    Write-Titulo "Validación automática ($($r.total) correlaciones esperadas, ~2,5 min)"
    do {
        Start-Sleep -Seconds 10
        Write-Host '.' -NoNewline
        $r = Invoke-Api 'GET' "$($u.Simulador)/api/validation/latest"
    } while ($r.status -eq 'RUNNING')
    Write-Host ''
    foreach ($x in @($r.results)) {
        $t = '{0,-22} -> {1,-24} zona {2,2}  {3}' -f $x.scenario, $x.expectedAlert, $x.zoneId, $x.detail
        if ($x.passed) { Write-Ok $t } else { Write-Fallo $t }
    }
    if ($r.status -eq 'PASSED') { Write-Ok "Resultado: $($r.passed)/$($r.total) PASSED" } else { Write-Fallo "Resultado: $($r.passed)/$($r.total) $($r.status)"; exit 1 }
}

# Pico y tráfico de fondo actúan en todos los generadores (C simula unas zonas y D otras), o en el de -Nodo
function Invoke-Pico {
    $cfg = Get-Config
    foreach ($s in @(Get-SimuladoresObjetivo $cfg)) {
        try {
            $null = Invoke-Api 'POST' ('{0}/api/simulator/burst?multiplier={1}&seconds={2}' -f $s.Url, $Multiplicador, $Segundos)
            Write-Ok "Nodo $($s.Nodo): pico x$Multiplicador durante $Segundos s."
        } catch { Write-Fallo "Nodo $($s.Nodo): $($_.Exception.Message)" }
    }
    Write-Info "Mírelo en Grafana: $((Get-Urls $cfg).Grafana)"
}

function Invoke-Fondo {
    $activar = ([string]$Objetivo).ToLower() -notin @('off', 'no', 'apagar', 'false', '0')
    $cfg = Get-Config
    foreach ($s in @(Get-SimuladoresObjetivo $cfg)) {
        try {
            $null = Invoke-Api 'POST' ('{0}/api/simulator/baseline?enabled={1}' -f $s.Url, $activar.ToString().ToLower())
            if ($activar) { Write-Ok "Nodo $($s.Nodo): tráfico de fondo encendido." } else { Write-Ok "Nodo $($s.Nodo): tráfico de fondo apagado." }
        } catch { Write-Fallo "Nodo $($s.Nodo): $($_.Exception.Message)" }
    }
    if (-not $activar) { Write-Info 'Los escenarios ya corren aislados del ruido de fondo; apagarlo solo deja la consola más tranquila.' }
}

function Invoke-Replay {
    $u = Get-Urls (Get-Config)
    $recurso = if ($Objetivo) { $Objetivo } else { 'noche-de-viernes.csv' }
    $url = '{0}/api/replay?resource={1}&speed={2}&jitter=true' -f $u.Simulador, $recurso, (Get-Velocidad)
    $r = Invoke-Api 'POST' $url @{} 60
    Write-Ok "Replay de $recurso lanzado: $($r | ConvertTo-Json -Compress)"
}

# Reporte ciudadano o pánico como los manda la app del celular: por la puerta HTTP de la ingesta (nodo B)
function Send-App([string]$Ruta, [hashtable]$Cuerpo) {
    $u = Get-Urls (Get-Config)
    $json = $Cuerpo | ConvertTo-Json -Depth 4 -Compress
    # Se mandan los bytes en UTF-8: con un texto, PowerShell 5.1 lo enviaría en otra codificación y se perderían las tildes
    $bytes = [Text.Encoding]::UTF8.GetBytes($json)
    try {
        return Invoke-RestMethod -Method Post -Uri "$($u.Ingesta)$Ruta" -Body $bytes -ContentType 'application/json; charset=utf-8' `
            -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop
    } catch {
        $detalle = $_.Exception.Message
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $detalle = $_.ErrorDetails.Message }
        Stop-Centinela "La ingesta no aceptó el evento ($($u.Ingesta)$Ruta): $detalle"
    }
}

function Get-ZonaDemo([int]$PorDefecto) {
    $z = if ($Zona -gt 0) { $Zona } else { $PorDefecto }
    if ($z -lt 1 -or $z -gt 12) { Stop-Centinela "Zona inválida: $z (de 1 a 12)." }
    return $z
}

function Invoke-Reporte {
    if (-not $Objetivo) { Stop-Centinela 'Escriba el texto, p. ej.: centinela.cmd reporte "Asaltaron a una señora en la parada" -Zona 4 -Tipo ROBO' }
    $zona = Get-ZonaDemo 4
    $tipo = if ($Tipo) { $Tipo.Trim().ToUpper() } else { 'ROBO' }
    $validos = @('ROBO', 'ACCIDENTE', 'INCENDIO', 'ALTERCADO', 'PERSONA_SOSPECHOSA', 'OTRO')
    if ($tipo -notin $validos) { Stop-Centinela "Tipo de reporte inválido: $tipo (use $($validos -join ', '))." }
    $r = Send-App '/api/v1/citizen-reports' @{
        tipo_evento = $tipo; mensaje_descriptivo = $Objetivo; origen = 'APP'
        reportante_id = 'CIU-CLI-{0:D5}' -f (Get-Random -Maximum 100000)
        ubicacion_aproximada = @{ nombre = "Zona $zona" }
    }
    Write-Ok "Reporte ciudadano $tipo en la Zona $zona recibido ($($r.estado), evento $($r.event_id))."
    Write-Info 'La consola lo muestra con su texto (placas enmascaradas) en la evidencia de la alerta que lo use; un reporte'
    Write-Info 'de ACCIDENTE o INCENDIO sin otras señales queda a los 90 s como posible falso positivo.'
}

function Invoke-Panico {
    $zona = Get-ZonaDemo 10
    $tipo = if ($Tipo) { $Tipo.Trim().ToUpper() } else { 'PANICO' }
    $validos = @('PANICO', 'ROBO', 'ASALTO', 'EMERGENCIA_MEDICA', 'INCENDIO', 'ACCIDENTE')
    if ($tipo -notin $validos) { Stop-Centinela "Tipo de pánico inválido: $tipo (use $($validos -join ', '))." }
    $r = Send-App '/api/v1/panic' @{
        tipo_alerta = $tipo; identificador_dispositivo = 'APP-CLI{0:D5}' -f (Get-Random -Maximum 100000)
        zona_geografica = @{ nombre = "Zona $zona" }
        user_context = @{ usuario_id = 'CIU-CLI-{0:D5}' -f (Get-Random -Maximum 100000); plataforma = 'WINDOWS'; version_app = 'centinela.cmd' }
    }
    Write-Ok "Botón de pánico $tipo en la Zona $zona recibido ($($r.estado), evento $($r.event_id))."
    if ($tipo -in @('PANICO', 'ROBO', 'ASALTO')) { Write-Info 'Si nada lo corrobora en la zona, a los 90 s queda como posible falso positivo (fila 2 de la tabla 2.4).' }
}

function Invoke-Tarea {
    $validas = @('rule-quality', 'slo', 'plate-retention', 'alert-expiry', 'watchlist')
    if ($Objetivo -notin $validas) { Stop-Centinela "Indique la tarea: $($validas -join ', ')" }
    $cfg = Get-Config
    $u = Get-Urls $cfg
    $r = Invoke-Api 'POST' "$($u.Consola)/api/admin/jobs/$Objetivo" (Get-AutorizacionAdmin $cfg) 120
    Write-Ok "Tarea '$Objetivo' ejecutada: $($r | ConvertTo-Json -Compress)"
}

function Invoke-ReiniciarCorrelacion {
    if (-not (Read-SiNo 'Se borra el estado temporal del motor (ventanas e incidentes abiertos). ¿Continuar?' $false)) { return }
    $cfg = Get-Config
    $u = Get-Urls $cfg
    $null = Invoke-Api 'POST' "$($u.Consola)/api/admin/correlation/reset" (Get-AutorizacionAdmin $cfg)
    Write-Ok 'Estado de correlación reiniciado (queda registrado en la auditoría).'
}

# Servicios de cada nodo dentro del modo de una sola PC (para simular caídas)
$ServiciosPorNodo = @{
    A = @('kafka', 'timescaledb', 'redis', 'db-backup')
    B = @('ingestion-service', 'correlation-engine', 'alert-service', 'prometheus', 'grafana')
    C = @('sensor-simulator')
}

function Invoke-Caida([bool]$Encender) {
    Assert-Docker
    $objetivo = ([string]$Objetivo).ToUpper()
    if ($objetivo -notin @('A', 'B', 'C', 'D')) { Stop-Centinela 'Indique el nodo: A, B o C (p. ej. centinela.cmd caida B).' }
    $cfg = Get-Config
    $roles = Get-Roles
    $accion = if ($Encender) { 'start' } else { 'stop' }
    if ($roles -contains 'UNICO') {
        $clave = if ($objetivo -eq 'D') { 'C' } else { $objetivo }
        $codigo = Invoke-Compose 'UNICO' $cfg (@($accion) + $ServiciosPorNodo[$clave])
    } elseif ($roles -contains $objetivo) {
        $codigo = Invoke-Compose $objetivo $cfg @($accion)
    } else {
        Stop-Centinela "El nodo $objetivo no corre en esta PC: ejecute el comando en la PC del nodo $objetivo."
    }
    if ($codigo -ne 0) { Stop-Centinela "docker compose $accion falló en el nodo $objetivo (ver arriba)." }
    if ($Encender) { Write-Ok "Nodo $objetivo encendido de nuevo." }
    else { Write-Ok "Nodo $objetivo apagado. Observe la consola, Grafana y el vigilante; luego: centinela.cmd recuperar $objetivo" }
}

# Ejecuta una herramienta de Kafka y devuelve su salida (stdout y stderr juntos)
function Invoke-Kafka([string[]]$Argumentos) {
    $cfg = Get-Config
    $roles = Get-RolesLocales
    if ($roles -contains 'UNICO') {
        $info = Get-InfoRol 'UNICO' $cfg
        $cmd = @('compose', '--env-file', $ArchivoEnv, '-f', $info.Archivo, '-p', $info.Proyecto, 'exec', '-T', 'kafka') + $Argumentos + @('--bootstrap-server', 'localhost:9092')
    } elseif ($roles -contains 'A') {
        $info = Get-InfoRol 'A' $cfg
        $cmd = @('compose', '--env-file', $ArchivoEnv, '-f', $info.Archivo, '-p', $info.Proyecto, 'exec', '-T', 'kafka') + $Argumentos + @('--bootstrap-server', 'localhost:29092')
    } else {
        # Desde cualquier otra PC: un contenedor desechable con las herramientas de Kafka
        $cmd = @('run', '--rm', 'apache/kafka:3.9.1') + $Argumentos + @('--bootstrap-server', ('{0}:{1}' -f $cfg['NODE_A_IP'], $cfg['KAFKA_PORT']))
    }
    return @(& $DockerExe @cmd 2>&1 | ForEach-Object { "$_" })
}

function Invoke-Particiones {
    Assert-Docker
    foreach ($g in @('ingestion', 'correlation', 'event-store', 'alert-store')) {
        Write-Titulo "Grupo de consumidores '$g' (CLIENT-ID dice en qué nodo está cada instancia)"
        Invoke-Kafka @('/opt/kafka/bin/kafka-consumer-groups.sh', '--describe', '--group', $g, '--members') |
            Where-Object { $_.Trim() } | ForEach-Object { Write-Info $_ }
    }
}

function Invoke-Dlq {
    Assert-Docker
    $cfg = Get-Config
    $u = Get-Urls $cfg
    try {
        $r = Invoke-Api 'GET' "$($u.Consola)/api/admin/dlq?ultimos=15" (Get-AutorizacionAdmin $cfg) 60
        Write-Titulo "Cola de mensajes muertos (events.dlq): $($r.total) mensajes"
        foreach ($p in $r.porOrigenClaseYServicio.PSObject.Properties) { Write-Info ('{0,5}  {1}' -f $p.Value, $p.Name) }
        if ($r.total -gt 0) { Write-Titulo 'Últimos' }
        foreach ($m in @($r.ultimos)) { Write-Info ('{0}  {1}  {2}  {3}' -f $m.hora, $m.topicOriginal, $m.clase, $m.causa) }
        if ($r.total -eq 0) { Write-Ok 'La DLQ está vacía.' }
        return
    } catch {
        Write-Aviso 'No se pudo consultar el alert-service; se lee la DLQ directo de Kafka.'
    }
    Write-Titulo 'Primeros 20 mensajes de events.dlq (con sus cabeceras: tópico original y causa)'
    $salida = Invoke-Kafka @('/opt/kafka/bin/kafka-console-consumer.sh', '--topic', 'events.dlq', '--from-beginning', '--timeout-ms', '6000', '--max-messages', '20', '--property', 'print.headers=true')
    # Al agotar la espera la herramienta imprime un TimeoutException: es normal, no es un error
    $mensajes = @($salida | Where-Object { $_ -notmatch 'TimeoutException|Error processing message|terminating consumer|^\s*at |Processed a total' -and $_.Trim() })
    if ($mensajes.Count -eq 0) { Write-Ok 'La DLQ está vacía.' } else { $mensajes | ForEach-Object { Write-Info $_ } }
}

function Invoke-ReprocesarDlq {
    $cfg = Get-Config
    $u = Get-Urls $cfg
    $url = "$($u.Consola)/api/admin/dlq/reprocess"
    if ($IncluirPermanentes) { $url += '?incluirPermanentes=true' }
    $r = Invoke-Api 'POST' $url (Get-AutorizacionAdmin $cfg) 300
    Write-Ok ("Reinyectados: {0}; omitidos: {1} (leídos {2})" -f $r.reprocessed, $r.skipped, $r.read)
    if (-not $IncluirPermanentes -and $r.skipped -gt 0) { Write-Info 'Los PERMANENTE (mensajes mal formados) se omiten; para incluirlos: reprocesar-dlq -IncluirPermanentes' }
}

function Get-RolDatos {
    $roles = Get-Roles
    if ($roles -contains 'UNICO') { return 'UNICO' }
    if ($roles -contains 'A') { return 'A' }
    Stop-Centinela 'Los respaldos se hacen en la PC del nodo A (o en el modo de una sola PC).'
}

function Invoke-Respaldo {
    Assert-Docker
    $rol = Get-RolDatos
    if ((Invoke-Compose $rol (Get-Config) @('exec', '-T', 'db-backup', 'sh', '/scripts/respaldo.sh', 'ahora')) -ne 0) { Stop-Centinela 'El respaldo falló (ver arriba).' }
    Write-Info "Respaldos en: $(Join-Path $Raiz 'backups')"
    Get-ChildItem -LiteralPath (Join-Path $Raiz 'backups') -Filter '*.dump' -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 5 | ForEach-Object { Write-Info ('{0}  {1:N0} KB' -f $_.Name, ($_.Length / 1KB)) }
}

function Invoke-Restaurar {
    Assert-Docker
    $rol = Get-RolDatos
    $dir = Join-Path $Raiz 'backups'
    $archivo = $Objetivo
    if (-not $archivo) {
        $ultimo = Get-ChildItem -LiteralPath $dir -Filter '*.dump' -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if (-not $ultimo) { Stop-Centinela "No hay respaldos en $dir" }
        $archivo = $ultimo.Name
    }
    $archivo = Split-Path -Leaf $archivo
    if (-not (Test-Path -LiteralPath (Join-Path $dir $archivo))) { Stop-Centinela "No existe $(Join-Path $dir $archivo)" }
    Write-Aviso "Se reemplazará la base de datos por el respaldo $archivo."
    Write-Info 'Se restaura en una base temporal y luego se intercambia: los servicios se reconectan solos.'
    if (-not (Read-SiNo '¿Continuar?' $false)) { return }
    if ((Invoke-Compose $rol (Get-Config) @('exec', '-T', 'db-backup', 'sh', '/scripts/respaldo.sh', 'restaurar', $archivo)) -ne 0) { Stop-Centinela 'La restauración falló (ver arriba); la base actual no se tocó.' }
}

# Maven dentro de un contenedor: no hace falta instalar Java ni Maven en Windows.
# El volumen centinela-m2 guarda las dependencias descargadas entre una ejecución y otra.
function Invoke-Maven([string[]]$Argumentos) {
    Assert-Docker
    & $DockerExe run --rm -v "${Raiz}:/src" -v 'centinela-m2:/root/.m2' -w /src maven:3.9.9-eclipse-temurin-21 mvn -B @Argumentos | Out-Host
    return $LASTEXITCODE
}

function Invoke-CompilarJar {
    Write-Titulo 'Compilando los 5 módulos con Maven dentro de Docker (la primera vez descarga dependencias)'
    if ((Invoke-Maven @('-q', 'package', '-DskipTests')) -ne 0) { Stop-Centinela 'La compilación falló.' }
    Save-Env ([ordered]@{ BUILD_TARGET = 'prebuilt' })
    Write-Ok 'Listo. Con BUILD_TARGET=prebuilt, "centinela.cmd iniciar -Compilar" crea las imágenes con esos jar en segundos.'
    Write-Info '(Si tiene Java 21 instalado, también sirve: mvnw.cmd package -DskipTests)'
}

function Invoke-Pruebas {
    Write-Titulo 'Pruebas automatizadas (Maven dentro de Docker)'
    $argumentos = @('test')
    if ($Objetivo) { $argumentos = @('-pl', $Objetivo, '-am', 'test', '-Dsurefire.failIfNoSpecifiedTests=false') }
    $codigo = Invoke-Maven $argumentos
    if ($codigo -eq 0) { Write-Ok 'Todas las pruebas pasaron.' } else { Stop-Centinela 'Hay pruebas que fallaron (ver arriba).' }
}

function Invoke-ExportarImagenes {
    Assert-Docker
    $archivo = if ($Objetivo) { $Objetivo } else { Join-Path $Raiz 'centinela-imagenes.tar' }
    if (-not (Test-Path -LiteralPath $ArchivoEnv)) { Save-Env ([ordered]@{}) }   # compose necesita un .env
    $cfg = Get-Config
    Write-Titulo 'Construyendo las imágenes del proyecto'
    if ((Invoke-Compose 'UNICO' $cfg @('build')) -ne 0) { Stop-Centinela 'Falló la construcción.' }
    foreach ($i in $ImagenesBase) { & $DockerExe pull $i | Out-Host }
    Write-Titulo "Guardando en $archivo"
    & $DockerExe save -o $archivo @($ImagenesPropias + $ImagenesBase) | Out-Host
    if ($LASTEXITCODE -ne 0) { Stop-Centinela 'docker save falló (ver arriba): revise el espacio libre en disco.' }
    Write-Ok "Listo: copie $archivo a cada PC y ejecute 'centinela.cmd importar-imagenes'."
}

function Invoke-ImportarImagenes {
    Assert-Docker
    $archivo = if ($Objetivo) { $Objetivo } else { Join-Path $Raiz 'centinela-imagenes.tar' }
    if (-not (Test-Path -LiteralPath $archivo)) { Stop-Centinela "No existe $archivo" }
    & $DockerExe load -i $archivo | Out-Host
    if ($LASTEXITCODE -ne 0) { Stop-Centinela 'docker load falló (ver arriba): el archivo puede estar incompleto; cópielo de nuevo.' }
    Write-Ok 'Imágenes cargadas. Siguiente paso: centinela.cmd iniciar  (sin -Compilar: ya no hace falta construir nada).'
}

# Menú para el doble clic en centinela.cmd
function Show-Menu {
    $opciones = [ordered]@{
        '1' = @('configurar', 'Configurar esta PC (una sola PC o varias)')
        '2' = @('iniciar', 'Iniciar')
        '3' = @('estado', 'Ver estado y salud')
        '4' = @('abrir', 'Abrir consola, Grafana y vigilante')
        '5' = @('validar', 'Validación automática (9 correlaciones)')
        '6' = @('escenario', 'Lanzar el escenario de la tabla 2.4')
        '7' = @('puertos', 'Revisar puertos')
        '8' = @('detener', 'Detener')
        '9' = @('ayuda', 'Ver todos los comandos')
    }
    Write-Host ''
    Write-Host '  Centinela Urbano' -ForegroundColor Cyan
    foreach ($k in $opciones.Keys) { Write-Host ("   {0}) {1}" -f $k, $opciones[$k][1]) }
    $r = Read-Host '  Elija una opción (Enter para salir)'
    if (-not $opciones.Contains($r)) { return $null }
    if ($r -eq '6') { $script:Objetivo = 'tabla-2-4' }
    return $opciones[$r][0]
}

# =============================================================================================
# Despacho
# =============================================================================================
if ($Comando -eq 'menu') {
    $elegido = Show-Menu
    if (-not $elegido) { exit 0 }
    $Comando = $elegido
}
switch ($Comando.ToLower()) {
    'ayuda' { Show-Ayuda }
    'help' { Show-Ayuda }
    'configurar' { Invoke-Configurar }
    'requisitos' { Invoke-Requisitos }
    'puertos' { Invoke-Puertos }
    'firewall' { Invoke-ComandoFirewall }
    'compilar' { Invoke-CompilarJar }
    'pruebas' { Invoke-Pruebas }
    'fondo' { Invoke-Fondo }
    'replay' { Invoke-Replay }
    'tarea' { Invoke-Tarea }
    'reiniciar-correlacion' { Invoke-ReiniciarCorrelacion }
    'iniciar' { Invoke-Iniciar }
    'detener' { Invoke-Detener }
    'estado' { Invoke-Estado }
    'logs' { Invoke-Logs }
    'abrir' { Invoke-Abrir }
    'escenarios' { Invoke-Escenarios }
    'escenario' { Invoke-Escenario }
    'validar' { Invoke-Validar }
    'reporte' { Invoke-Reporte }
    'panico' { Invoke-Panico }
    'pico' { Invoke-Pico }
    'caida' { Invoke-Caida $false }
    'recuperar' { Invoke-Caida $true }
    'particiones' { Invoke-Particiones }
    'dlq' { Invoke-Dlq }
    'reprocesar-dlq' { Invoke-ReprocesarDlq }
    'respaldo' { Invoke-Respaldo }
    'restaurar' { Invoke-Restaurar }
    'exportar-imagenes' { Invoke-ExportarImagenes }
    'importar-imagenes' { Invoke-ImportarImagenes }
    default { Write-Fallo "Comando desconocido: $Comando"; Show-Ayuda; exit 1 }
}
if ($Pausa) { Read-Host 'Presione Enter para cerrar' | Out-Null }
