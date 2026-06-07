#!/usr/bin/env python3
"""
Audio transcription using OpenAI Whisper.
Supports mp3, wav, m4a, flac, ogg, and other ffmpeg-compatible formats.
"""

import os
import argparse
from pathlib import Path
from typing import Optional


def transcribe(
    audio_path: str,
    model_size: str = "base",
    language: Optional[str] = None,
    output_path: Optional[str] = None,
) -> str:
    """
    Transcribe an audio file using Whisper.

    Args:
        audio_path: Path to the audio file.
        model_size: Whisper model size — tiny, base, small, medium, large.
                    Larger models are more accurate but slower.
        language: ISO-639-1 language code (e.g. "en"). Auto-detected if None.
        output_path: If provided, write the transcript to this file path.

    Returns:
        Transcribed text as a string.
    """
    import whisper

    if not os.path.exists(audio_path):
        raise FileNotFoundError(f"Audio file not found: {audio_path}")

    print(f"Loading Whisper model '{model_size}'...")
    model = whisper.load_model(model_size)

    print(f"Transcribing: {audio_path}")
    result = model.transcribe(audio_path, language=language)
    text: str = result["text"].strip()

    if output_path:
        Path(output_path).write_text(text, encoding="utf-8")
        print(f"Transcript saved to: {output_path}")

    return text


def main():
    parser = argparse.ArgumentParser(
        description="Transcribe an audio file using OpenAI Whisper"
    )
    parser.add_argument("audio", help="Path to the audio file")
    parser.add_argument(
        "--model",
        "-m",
        default="base",
        choices=["tiny", "base", "small", "medium", "large"],
        help="Whisper model size (default: base)",
    )
    parser.add_argument(
        "--language",
        "-l",
        default=None,
        help="Language code, e.g. 'en' (auto-detected if omitted)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Save transcript to this file path",
    )
    args = parser.parse_args()

    transcript = transcribe(
        audio_path=args.audio,
        model_size=args.model,
        language=args.language,
        output_path=args.output,
    )
    print("\n--- Transcript ---")
    print(transcript)


if __name__ == "__main__":
    main()
