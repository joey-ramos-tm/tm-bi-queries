import React from 'react'
import { ChevronRight } from 'lucide-react'

interface HeaderProps {
  title: string
  subtitle?: string
  breadcrumbs?: Array<{ label: string; href?: string }>
}

const Header: React.FC<HeaderProps> = ({ title, subtitle, breadcrumbs }) => {
  return (
    <div className="mb-6">
      {/* Breadcrumbs */}
      {breadcrumbs && breadcrumbs.length > 0 && (
        <div className="flex items-center space-x-2 text-sm text-gray-500 mb-2">
          {breadcrumbs.map((crumb, index) => (
            <React.Fragment key={index}>
              {index > 0 && <ChevronRight className="h-4 w-4" />}
              {crumb.href ? (
                <a
                  href={crumb.href}
                  className="hover:text-primary transition-colors"
                >
                  {crumb.label}
                </a>
              ) : (
                <span className="text-gray-900 font-medium">{crumb.label}</span>
              )}
            </React.Fragment>
          ))}
        </div>
      )}

      {/* Title */}
      <h1 className="text-3xl font-bold text-gray-900">{title}</h1>

      {/* Subtitle */}
      {subtitle && <p className="text-gray-600 mt-1">{subtitle}</p>}
    </div>
  )
}

export default Header
