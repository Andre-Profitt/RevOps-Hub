/**
 * Foundry API Route Handler
 *
 * Server-side proxy for Foundry API calls. Keeps credentials secure
 * by never exposing them to the browser.
 */

import { NextRequest, NextResponse } from 'next/server'

// Server-only env vars (no NEXT_PUBLIC_ prefix)
const FOUNDRY_URL = process.env.FOUNDRY_URL || ''
const FOUNDRY_TOKEN = process.env.FOUNDRY_TOKEN || ''

export async function POST(request: NextRequest) {
  // Validate server configuration
  if (!FOUNDRY_URL || !FOUNDRY_TOKEN) {
    return NextResponse.json(
      { error: 'Foundry not configured', useMock: true },
      { status: 503 }
    )
  }

  try {
    const body = await request.json()
    const { query, fallbackBranchIds = ['master'] } = body

    if (!query) {
      return NextResponse.json(
        { error: 'Query is required' },
        { status: 400 }
      )
    }

    const response = await fetch(`${FOUNDRY_URL}/api/v2/datasets/sql/execute`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${FOUNDRY_TOKEN}`,
      },
      body: JSON.stringify({
        query,
        fallbackBranchIds,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      return NextResponse.json(
        { error: `Foundry SQL error: ${error}` },
        { status: response.status }
      )
    }

    const result = await response.json()
    return NextResponse.json(result)
  } catch (error) {
    console.error('[Foundry API] Error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function GET() {
  // Health check endpoint
  const configured = !!(FOUNDRY_URL && FOUNDRY_TOKEN)
  return NextResponse.json({
    configured,
    status: configured ? 'ready' : 'mock_mode',
  })
}
