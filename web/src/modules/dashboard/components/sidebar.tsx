"use client";

import * as React from "react";
import { cn } from "@/lib/utils";

/**
 * Mux-style sidebar.
 *
 * Unlike stock shadcn/ui sidebar (which toggles between expanded/collapsed
 * on click), this rail stays permanently collapsed to an icon-only width
 * and expands as a hover flyout that overlays the page content rather than
 * pushing it — matching mux.com/app's dashboard nav. The expand/collapse
 * itself needs no React state: it's a CSS `group-hover` width transition,
 * so it's cheap and can't get out of sync. React state is only used for
 * the mobile off-canvas open/close.
 */

type SidebarContextValue = {
  mobileOpen: boolean;
  setMobileOpen: (open: boolean) => void;
};

const SidebarContext = React.createContext<SidebarContextValue | null>(null);

export function useSidebar() {
  const ctx = React.useContext(SidebarContext);
  if (!ctx) {
    throw new Error("useSidebar must be used within a <SidebarProvider>");
  }
  return ctx;
}

export function SidebarProvider({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  const [mobileOpen, setMobileOpen] = React.useState(false);

  return (
    <SidebarContext.Provider value={{ mobileOpen, setMobileOpen }}>
      <div className={cn("flex min-h-svh w-full", className)}>{children}</div>
    </SidebarContext.Provider>
  );
}

export function Sidebar({ className, children }: React.ComponentProps<"aside">) {
  const { mobileOpen, setMobileOpen } = useSidebar();

  return (
    <>
      {/* Scrim behind the off-canvas sidebar on mobile */}
      <div
        aria-hidden
        onClick={() => setMobileOpen(false)}
        className={cn(
          "fixed inset-0 z-40 bg-black/40 transition-opacity md:hidden",
          mobileOpen ? "opacity-100" : "pointer-events-none opacity-0",
        )}
      />
      <aside
        data-slot="sidebar"
        data-mobile-open={mobileOpen}
        className={cn(
          // Collapsed icon rail by default; on desktop, hovering the rail
          // grows it and it overlays content (fixed + z-50), so layout
          // underneath never shifts.
          "group/sidebar fixed inset-y-0 left-0 z-50 flex h-svh w-[var(--sidebar-width-icon)] flex-col overflow-hidden border-r border-sidebar-border bg-sidebar text-sidebar-foreground shadow-sm transition-[width] duration-200 ease-in-out will-change-[width]",
          "md:hover:w-[var(--sidebar-width)] md:hover:shadow-lg",
          // Mobile: full off-canvas panel, slid in/out with translate.
          "max-md:w-[var(--sidebar-width)] max-md:-translate-x-full max-md:transition-transform",
          mobileOpen && "max-md:translate-x-0",
          className,
        )}
      >
        {children}
      </aside>
    </>
  );
}

export function SidebarInset({ className, children }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "flex min-h-svh w-full flex-1 flex-col bg-background md:pl-[var(--sidebar-width-icon)]",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function SidebarTrigger({ className, ...props }: React.ComponentProps<"button">) {
  const { mobileOpen, setMobileOpen } = useSidebar();
  return (
    <button
      type="button"
      aria-label="Toggle sidebar"
      onClick={() => setMobileOpen(!mobileOpen)}
      className={cn(
        "inline-flex size-8 items-center justify-center rounded-md text-foreground hover:bg-accent md:hidden",
        className,
      )}
      {...props}
    >
      <svg viewBox="0 0 24 24" fill="none" className="size-5">
        <path
          d="M4 6h16M4 12h16M4 18h16"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
        />
      </svg>
    </button>
  );
}

export function SidebarHeader({ className, children }: React.ComponentProps<"div">) {
  return (
    <div className={cn("flex h-14 shrink-0 items-center px-4", className)}>{children}</div>
  );
}

export function SidebarContent({ className, children }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto overflow-x-hidden px-2 py-2",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function SidebarFooter({ className, children }: React.ComponentProps<"div">) {
  return <div className={cn("flex shrink-0 flex-col gap-1 px-2 py-2", className)}>{children}</div>;
}

export function SidebarGroup({ className, children }: React.ComponentProps<"div">) {
  return <div className={cn("flex flex-col gap-1", className)}>{children}</div>;
}

export function SidebarGroupLabel({ className, children }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "px-2.5 pb-1.5 text-[11px] font-semibold tracking-wide whitespace-nowrap text-muted-foreground uppercase",
        "opacity-0 transition-opacity delay-75 duration-150 group-hover/sidebar:opacity-100 max-md:opacity-100",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function SidebarMenu({ className, children }: React.ComponentProps<"ul">) {
  return <ul className={cn("flex flex-col gap-0.5", className)}>{children}</ul>;
}

export function SidebarMenuItem({ className, children }: React.ComponentProps<"li">) {
  return <li className={cn("relative", className)}>{children}</li>;
}

type SidebarMenuButtonProps = Omit<React.ComponentProps<"a">, "href" | "type"> &
  Omit<React.ComponentProps<"button">, "type"> & {
    className?: string;
    isActive?: boolean;
    icon?: React.ReactNode;
    children?: React.ReactNode;
    href?: string;
    type?: "button" | "submit" | "reset";
  };

export function SidebarMenuButton({
  className,
  isActive,
  icon,
  children,
  href,
  ...props
}: SidebarMenuButtonProps) {
  const classes = cn(
    "group/button relative flex h-9 w-full items-center gap-2.5 rounded-lg px-2.5 text-sm font-medium text-sidebar-foreground/80 transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
    "md:justify-center md:group-hover/sidebar:justify-start",
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sidebar-ring focus-visible:ring-offset-1 focus-visible:ring-offset-sidebar",
    "data-[active=true]:bg-sidebar-accent data-[active=true]:text-sidebar-accent-foreground",
    className,
  );

  const content = (
    <>
      {isActive ? (
        <span
          className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-sidebar-primary"
          aria-hidden
        />
      ) : null}
      {icon ? (
        <span className="flex size-6 shrink-0 items-center justify-center [&>svg]:size-[22px]">
          {icon}
        </span>
      ) : null}
      <span
        className={cn(
          "truncate whitespace-nowrap text-sidebar-foreground/80",
          "opacity-0 transition-opacity delay-75 duration-150",
          "md:hidden md:group-hover/sidebar:block md:group-hover/sidebar:opacity-100 max-md:opacity-100",
        )}
      >
        {children}
      </span>
    </>
  );

  if (href) {
    return (
      <a
        href={href}
        data-active={isActive}
        aria-current={isActive ? "page" : undefined}
        className={classes}
        {...props}
      >
        {content}
      </a>
    );
  }

  return (
    <button type="button" data-active={isActive} className={classes} {...props}>
      {content}
    </button>
  );
}
