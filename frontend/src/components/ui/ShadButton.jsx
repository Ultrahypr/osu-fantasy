import React from 'react'
import clsx from 'clsx'

const VARIANTS = {
  primary: 'bg-indigo-600 text-white hover:bg-indigo-700 focus:ring-indigo-400',
  secondary: 'bg-white bg-opacity-10 text-white hover:bg-opacity-20',
  ghost: 'bg-transparent text-white/90 hover:bg-white/5',
}

export default function ShadButton({ children, variant = 'primary', as = 'button', className = '', ...props }){
  const base = 'button-texture inline-flex items-center justify-center rounded-md px-4 py-2 text-sm font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 disabled:opacity-50 disabled:pointer-events-none'
  const variantClass = VARIANTS[variant] || VARIANTS.primary
  const classes = clsx(base, variantClass, className)

  if(as === 'a'){
    return <a className={classes} {...props}>{children}</a>
  }

  return <button className={classes} {...props}>{children}</button>
}
