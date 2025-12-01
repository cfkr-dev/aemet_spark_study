import type {Metadata} from "next";
import "./globals.css";

export const metadata: Metadata = {
    title: "Meteorological dashboard",
    description: "Dashboard to show meteorological studies results",
    icons: {
        icon: "/icon.svg",
    },
};

export default function RootLayout({
                                       children,
                                   }: Readonly<{
    children: React.ReactNode;
}>) {
    return (
        <html lang="en">
        <body
            className="antialiased"
        >
        {children}
        </body>
        </html>
    );
}
