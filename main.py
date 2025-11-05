#!/usr/bin/env python3
"""
Leitor de QR codes pela webcam.

Funcionalidades:
- Abre a webcam (device 0)
- Detecta QR codes (usa pyzbar se disponível ou OpenCV QRCodeDetector)
- Exibe overlay na janela com "QR CODE LIDO" e o conteúdo lido
- Evita leituras duplicadas (cooldown configurável)
- Toca uma notificação (notify-send) se disponível e emite um beep
- Salva cada leitura nova em um CSV com timestamp

Uso:
	python3 main.py

Pressione 'q' para sair.
"""

import csv
import os
import time
from datetime import datetime
import subprocess
import shutil

import cv2

try:
	from pyzbar import pyzbar
	_HAVE_PYZBAR = True
except Exception:
	_HAVE_PYZBAR = False


CSV_FILE = "lidos.csv"
COOLDOWN_SECONDS = 5  # evitar registrar o mesmo QR em menos que isso
OVERLAY_SECONDS = 2   # quanto tempo mostrar o overlay após leitura


def ensure_csv_header(path: str):
	if not os.path.exists(path):
		with open(path, "w", newline="", encoding="utf-8") as f:
			writer = csv.writer(f)
			writer.writerow(["timestamp", "data"])


def append_csv(path: str, data: str):
	with open(path, "a", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		writer.writerow([datetime.now().isoformat(timespec="seconds"), data])


def notify_linux(summary: str, body: str):
	# tenta usar notify-send quando disponível (Linux)
	if shutil.which("notify-send"):
		try:
			subprocess.Popen(["notify-send", summary, body])
		except Exception:
			pass


def main():
	ensure_csv_header(CSV_FILE)

	cap = cv2.VideoCapture(0)
	if not cap.isOpened():
		print("Erro: não foi possível abrir a webcam (device 0).")
		return

	print("Abrindo webcam. Pressione 'q' para sair.")

	# estado para evitar leituras duplicadas frequentes
	last_seen = {}  # data -> last seen timestamp
	display_message = ""
	display_expires = 0

	# detector OpenCV (fallback caso pyzbar não esteja instalado)
	qr_detector = None
	if not _HAVE_PYZBAR:
		qr_detector = cv2.QRCodeDetector()

	try:
		while True:
			ret, frame = cap.read()
			if not ret:
				print("Falha ao capturar frame da webcam")
				break

			found_any = False

			if _HAVE_PYZBAR:
				barcodes = pyzbar.decode(frame)
				for barcode in barcodes:
					data = barcode.data.decode("utf-8")
					found_any = True
					now = time.time()
					last = last_seen.get(data, 0)
					if now - last > COOLDOWN_SECONDS:
						print(f"QR CODE LIDO: {data}")
						append_csv(CSV_FILE, data)
						last_seen[data] = now
						display_message = f"QR CODE LIDO: {data}"
						display_expires = now + OVERLAY_SECONDS
						# notificar
						notify_linux("QR code lido", data)
						# beep terminal
						print('\a', end='')
					# desenhar retângulo/contorno e texto
					(x, y, w, h) = barcode.rect
					cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
					cv2.putText(frame, data, (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
			else:
				# usar OpenCV detector (pode detectar apenas 1 por vez em algumas versões)
				data, points, _ = qr_detector.detectAndDecode(frame)
				if data:
					found_any = True
					now = time.time()
					last = last_seen.get(data, 0)
					if now - last > COOLDOWN_SECONDS:
						print(f"QR CODE LIDO: {data}")
						append_csv(CSV_FILE, data)
						last_seen[data] = now
						display_message = f"QR CODE LIDO: {data}"
						display_expires = now + OVERLAY_SECONDS
						notify_linux("QR code lido", data)
						print('\a', end='')
					if points is not None:
						pts = points.astype(int).reshape(-1, 2)
						for i in range(len(pts)):
							pt1 = tuple(pts[i])
							pt2 = tuple(pts[(i + 1) % len(pts)])
							cv2.line(frame, pt1, pt2, (255, 0, 0), 2)

			# overlay de debug/feedback
			now = time.time()
			if now < display_expires and display_message:
				cv2.putText(frame, display_message, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)

			# mostrar contagem de itens lidos recentemente
			recent_count = sum(1 for t in last_seen.values() if now - t < 60)
			cv2.putText(frame, f"Lidos (últimos 60s): {recent_count}", (10, frame.shape[0] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (200, 200, 200), 1)

			cv2.imshow("Leitor QR - AutoCheckRH", frame)

			key = cv2.waitKey(1) & 0xFF
			if key == ord('q'):
				print("Saindo...")
				break

	finally:
		cap.release()
		cv2.destroyAllWindows()


if __name__ == "__main__":
	main()
