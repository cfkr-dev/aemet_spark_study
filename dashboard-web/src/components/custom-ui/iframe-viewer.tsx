interface IframeViewerProps {
    src: string;
    className?: string;
    onLoad?: () => void;
}

export default function IframeViewer({ src, className, onLoad }: IframeViewerProps) {
    return (
        <iframe
            src={src}
            className={`${className || ""}`}
            allowFullScreen
            onLoad={onLoad}
        ></iframe>
    );
}
