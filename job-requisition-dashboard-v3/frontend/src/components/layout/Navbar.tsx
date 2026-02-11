import React from 'react'
import { Link, useLocation } from 'react-router-dom'
import { RefreshCw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useQueryClient } from '@tanstack/react-query'

const Navbar: React.FC = () => {
  const location = useLocation()
  const queryClient = useQueryClient()

  const handleRefresh = () => {
    queryClient.invalidateQueries()
  }

  const isActive = (path: string) => {
    return location.pathname === path
  }

  return (
    <nav className="bg-white border-b border-gray-200 shadow-sm">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          {/* Logo and Title */}
          <div className="flex items-center space-x-8">
            <div className="flex items-center space-x-3">
              <div className="w-10 h-10 bg-primary rounded flex items-center justify-center">
                <span className="text-white font-bold text-xl">TM</span>
              </div>
              <div>
                <h1 className="text-xl font-bold text-gray-900">
                  Job Requisition Dashboard
                </h1>
                <p className="text-xs text-gray-500">Taylor Morrison</p>
              </div>
            </div>

            {/* Navigation Links */}
            <div className="flex space-x-1">
              <Link to="/dashboard">
                <Button
                  variant={isActive('/dashboard') ? 'default' : 'ghost'}
                  size="sm"
                >
                  Dashboard
                </Button>
              </Link>
              <Link to="/requisitions">
                <Button
                  variant={isActive('/requisitions') ? 'default' : 'ghost'}
                  size="sm"
                >
                  Requisitions
                </Button>
              </Link>
            </div>
          </div>

          {/* Actions */}
          <div className="flex items-center space-x-4">
            <Button
              variant="outline"
              size="sm"
              onClick={handleRefresh}
              className="flex items-center space-x-2"
            >
              <RefreshCw className="h-4 w-4" />
              <span>Refresh</span>
            </Button>
          </div>
        </div>
      </div>
    </nav>
  )
}

export default Navbar
