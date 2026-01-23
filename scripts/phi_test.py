"""Quick extractor test for Phi-3 Mini Instruct.

Usage:
  python3 scripts/phi_test.py --prompt "ICE arrested 5 people in Dallas on Monday."
  python3 scripts/phi_test.py --load-in-4bit
  python3 scripts/phi_test.py --temperature 0.1 --stop-text "### Problem"
"""

from __future__ import annotations

import argparse
import os

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer


DEFAULT_PROMPT = (
    "You are an extractor. Who/what/where from: "
    "ICE arrested 5 people in Dallas on Monday."
)
DEFAULT_MODEL_ID = "microsoft/Phi-3-mini-128k-instruct"


def load_model(model_id: str, load_in_4bit: bool, dtype: torch.dtype) -> AutoModelForCausalLM:
    """Load the model with optional 4-bit quantization."""
    if load_in_4bit:
        return AutoModelForCausalLM.from_pretrained(
            model_id,
            load_in_4bit=True,
            device_map="auto",
        )

    return AutoModelForCausalLM.from_pretrained(
        model_id,
        torch_dtype=dtype,
        device_map="auto",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Test Phi-3 Mini Instruct extraction.")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT, help="Prompt to run.")
    parser.add_argument(
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help="Model to load from Hugging Face.",
    )
    parser.add_argument(
        "--max-new-tokens",
        type=int,
        default=80,
        help="Max tokens to generate.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="Sampling temperature. Use 0 for deterministic greedy decoding.",
    )
    parser.add_argument(
        "--repetition-penalty",
        type=float,
        default=1.0,
        help="Penalty >1.0 discourages repetition.",
    )
    parser.add_argument(
        "--stop-text",
        default=None,
        help="If set, truncate output at the first occurrence of this string.",
    )
    parser.add_argument(
        "--load-in-4bit",
        action="store_true",
        help="Load the model in 4-bit (saves VRAM).",
    )
    args = parser.parse_args()

    # If fast transfer is enabled in env but the package is missing, disable it to avoid import errors.
    if os.environ.get("HF_HUB_ENABLE_HF_TRANSFER") == "1":
        try:
            import hf_transfer  # type: ignore  # noqa: F401
        except ModuleNotFoundError:
            os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"

    dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32
    tokenizer = AutoTokenizer.from_pretrained(args.model_id)
    model = load_model(args.model_id, args.load_in_4bit, dtype)

    inputs = tokenizer(args.prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(
        **inputs,
        max_new_tokens=args.max_new_tokens,
        do_sample=args.temperature > 0,
        temperature=args.temperature if args.temperature > 0 else None,
        repetition_penalty=args.repetition_penalty,
        eos_token_id=tokenizer.eos_token_id,
        pad_token_id=tokenizer.pad_token_id,
    )
    decoded = tokenizer.decode(outputs[0], skip_special_tokens=True)
    if args.stop_text:
        decoded = decoded.split(args.stop_text, 1)[0]
    print(decoded.strip())


if __name__ == "__main__":
    main()
