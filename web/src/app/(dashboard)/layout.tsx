import { AppSidebar } from "@/modules/dashboard/components/app-sidebar";
import {
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
} from "@/modules/dashboard/components/sidebar";

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset>
        <header className="flex h-14 shrink-0 items-center gap-3 border-b border-border px-4 md:hidden">
          <SidebarTrigger />
          <span className="text-sm font-semibold">Mux</span>
        </header>
        <main className="flex-1 px-6 py-8 md:px-10 md:py-10">{children}</main>
      </SidebarInset>
    </SidebarProvider>
  );
}
