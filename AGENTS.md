# AI Agent Instructions for audit_react

This file provides instructions and guidelines for AI agents working on the `audit_react` project.

## Project Architecture

This is a full-stack application for tax audit analysis (SEFIN).

*   **Frontend (`client/`)**: React 19 + TypeScript + Tailwind CSS 4 + shadcn/ui.
*   **Backend (`server/python/`)**: FastAPI + Polars + Parquet + Oracle DB.
*   **Shared (`shared/`)**: Constants shared between frontend and backend.

## Design Philosophy: "Institutional Precision" (Swiss Design Fiscal)

When generating or modifying UI components, strictly adhere to the following design principles:

*   **Grid & Layout**: Use a rigorous grid with mathematical alignment. The layout features a fixed dark sidebar (240px) on the left and a workspace area on the right. Tables should take up full width.
*   **Color Palette**:
    *   Sidebar Background: Deep slate (`#0f172a`)
    *   Workspace Background: Off-white (`#f8fafc`)
    *   Primary Action/Selection: Institutional Blue (`#1e40af`)
    *   Status Colors: Green (`#059669`) for success, Amber (`#d97706`) for pending/warnings, Red (`#dc2626`) for errors.
    *   Colors should be used functionally, never decoratively.
*   **Typography**:
    *   Display/Titles: DM Sans (Bold/700)
    *   Body/Labels: DM Sans (Regular/400 or Medium/500)
    *   Data/Monospace: JetBrains Mono (for CNPJs, numeric values, file names)
*   **Interactions**: Fast and subtle. Use 150ms-200ms fade-ins. Do not use bouncy or springy animations.
*   **Density**: Maximize visible data without visual pollution.

## Commands

*   **Frontend Development**: `pnpm dev`
*   **Backend Development**: `cd server/python && pip install -r requirements.txt && uvicorn api:app --reload --port 8000`
*   **Type Checking**: `pnpm check`
*   **Formatting**: `pnpm format`

## Rules

*   Before submitting any frontend changes, ensure type-checking passes by running `pnpm check`.
*   Ensure code is formatted using `pnpm format`.
*   Respect the component hierarchy and do not modify the build artifacts directly.
