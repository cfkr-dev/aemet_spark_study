import {NavigationCustomMenu} from "@/components/custom-ui/navigation-custom-menu";
import {CloudSun} from "lucide-react";
import StationStudies from "@/components/sections/stations";

export default function Home() {
    return (
        <div className="w-full min-h-screen flex flex-col">
            <header className="w-full flex items-center p-4 bg-background shadow-sm sticky top-0 z-50">
                <div className="flex gap-5 pl-5 items-center">
                    <h1 className="text-3xl font-bold shrink-0 w-max">Meteorological Dashboard</h1>
                    <CloudSun className="w-10 h-10 text-yellow-500" />
                </div>
                <div className="w-4/5 pl-20">
                    <NavigationCustomMenu></NavigationCustomMenu>
                </div>
            </header>

            <main className="flex-1 p-4">
                <StationStudies></StationStudies>
            </main>

        </div>
    )
}
