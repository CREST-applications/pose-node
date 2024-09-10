import rclpy
import os

from . import requester


def main():
    rclpy.init()

    config = requester.Config(
        camera_topic="/image_raw/compressed",
        pose_topic="/pose",
        pleiades_host=os.environ["PLEIADES_HOST"],
        max_job=4,
        max_fps=30,
    )

    requester_node = requester.PoseRequester(config)

    rclpy.spin(requester_node)

    requester_node.destroy_node()
    rclpy.shutdown()
