@echo off
title Analise Diaria — Dev Local
cd /d "C:\Users\paulorocha\analise-diaria"

echo Iniciando backend na porta 5001...
start "Backend Flask" cmd /k "python -X utf8 backend.py"

timeout /t 2 /nobreak > nul

echo Abrindo frontend...
start "" "http://127.0.0.1:5001/index.html"

echo.
echo Backend rodando em: http://localhost:5001
echo Feche a janela "Backend Flask" para parar.
