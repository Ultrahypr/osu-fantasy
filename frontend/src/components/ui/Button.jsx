import React from 'react'

export default function Button({ children, className = '', ...props }){
  return (
    <button
      className={"button-texture inline-flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium shadow-sm transition-colors focus:outline-none " + className}
      {...props}
    >
      {children}
    </button>
  )
}
