'use client'

import { AlertTriangle, RefreshCw, WifiOff } from 'lucide-react'

interface ErrorStateProps {
  title?: string
  message?: string
  onRetry?: () => void
  variant?: 'inline' | 'full' | 'card'
}

export function ErrorState({
  title = 'Failed to load data',
  message = 'There was a problem loading this data. Please try again.',
  onRetry,
  variant = 'card',
}: ErrorStateProps) {
  if (variant === 'inline') {
    return (
      <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 px-3 py-2 rounded-lg">
        <AlertTriangle className="w-4 h-4" />
        <span>{message}</span>
        {onRetry && (
          <button
            onClick={onRetry}
            className="ml-2 text-red-700 hover:text-red-800 underline"
          >
            Retry
          </button>
        )}
      </div>
    )
  }

  if (variant === 'full') {
    return (
      <div className="min-h-[400px] flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <WifiOff className="w-8 h-8 text-red-600" />
          </div>
          <h3 className="text-lg font-semibold text-gray-900 mb-2">{title}</h3>
          <p className="text-gray-500 mb-4">{message}</p>
          {onRetry && (
            <button
              onClick={onRetry}
              className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
              Try Again
            </button>
          )}
        </div>
      </div>
    )
  }

  // Default: card variant
  return (
    <div className="bg-white rounded-xl border border-red-200 p-6">
      <div className="flex items-start gap-4">
        <div className="w-10 h-10 bg-red-100 rounded-full flex items-center justify-center flex-shrink-0">
          <AlertTriangle className="w-5 h-5 text-red-600" />
        </div>
        <div className="flex-1">
          <h3 className="font-medium text-gray-900 mb-1">{title}</h3>
          <p className="text-sm text-gray-500 mb-3">{message}</p>
          {onRetry && (
            <button
              onClick={onRetry}
              className="inline-flex items-center gap-2 px-3 py-1.5 text-sm bg-red-50 text-red-700 rounded-lg hover:bg-red-100 transition-colors"
            >
              <RefreshCw className="w-3 h-3" />
              Retry
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

interface DataStateWrapperProps {
  loading: boolean
  error: Error | null
  onRetry?: () => void
  loadingComponent?: React.ReactNode
  children: React.ReactNode
}

export function DataStateWrapper({
  loading,
  error,
  onRetry,
  loadingComponent,
  children,
}: DataStateWrapperProps) {
  if (loading) {
    return <>{loadingComponent || <div className="animate-pulse">Loading...</div>}</>
  }

  if (error) {
    return (
      <ErrorState
        title="Failed to load data"
        message={error.message}
        onRetry={onRetry}
        variant="card"
      />
    )
  }

  return <>{children}</>
}
