"use client"

import { usePathname } from "next/navigation"
import {
  Clapperboard,
  Radio,
  ServerCog,
  Workflow,
  Gauge,
  BarChart3,
  PieChart,
  CircleAlert,
  Eye,
  Bell,
} from "lucide-react"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/modules/dashboard/components/sidebar"
import { UserButton } from "@/modules/auth/components/user-button"

const nav = [
  {
    label: "Video",
    items: [
      { title: "Assets", icon: Clapperboard, href: "/dashboard" },
      { title: "Live Streams", icon: Radio, href: "/dashboard/live-streams" },
    ],
  },
  {
    label: "Robots",
    items: [
      { title: "Jobs", icon: ServerCog, href: "/dashboard/jobs" },
      { title: "Directives", icon: Workflow, href: "/dashboard/directives" },
    ],
  },
  {
    label: "Data",
    items: [
      { title: "Overview", icon: Gauge, href: "/dashboard/overview" },
      { title: "Engagement", icon: BarChart3, href: "/dashboard/engagement" },
      { title: "Metrics", icon: PieChart, href: "/dashboard/metrics" },
      { title: "Errors", icon: CircleAlert, href: "/dashboard/errors" },
      { title: "Views", icon: Eye, href: "/dashboard/views" },
      { title: "Alerts", icon: Bell, href: "/dashboard/alerts" },
    ],
  },
]

export function AppSidebar() {
  const pathname = usePathname()
  const { setMobileOpen } = useSidebar()

  return (
    <Sidebar>
      <SidebarHeader>
        <a
          href="/dashboard"
          onClick={() => setMobileOpen(false)}
          className="flex items-center gap-2.5 md:justify-center md:group-hover/sidebar:justify-start"
        >
          <span className="flex size-6 shrink-0 items-center justify-center rounded-md bg-foreground text-background text-xs font-bold">
            M
          </span>
          <span className="truncate text-sm font-semibold tracking-tight opacity-0 transition-opacity delay-75 duration-150 md:hidden md:group-hover/sidebar:block md:group-hover/sidebar:opacity-100 max-md:opacity-100">
            Mux
          </span>
        </a>
      </SidebarHeader>

      <SidebarContent>
        {nav.map((group) => (
          <SidebarGroup key={group.label}>
            <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
            <SidebarMenu>
              {group.items.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton
                    href={item.href}
                    isActive={pathname === item.href}
                    icon={<item.icon />}
                    onClick={() => setMobileOpen(false)}
                  >
                    {item.title}
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroup>
        ))}
      </SidebarContent>
      <SidebarFooter>
        <UserButton />
      </SidebarFooter>
    </Sidebar>
  )
}
