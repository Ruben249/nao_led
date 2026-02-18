// Copyright 2024 Antonio Bono
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "nao_led_server/led_action_server.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "nao_led_interfaces/action/leds_play.hpp"
#include "nao_led_interfaces/msg/led_indexes.hpp"
#include "nao_led_interfaces/msg/led_modes.hpp"
#include "nao_lola_command_msgs/msg/chest_led.hpp"
#include "nao_lola_command_msgs/msg/head_leds.hpp"
#include "nao_lola_command_msgs/msg/left_ear_leds.hpp"
#include "nao_lola_command_msgs/msg/left_eye_leds.hpp"
#include "nao_lola_command_msgs/msg/left_foot_led.hpp"
#include "nao_lola_command_msgs/msg/right_ear_leds.hpp"
#include "nao_lola_command_msgs/msg/right_eye_leds.hpp"
#include "nao_lola_command_msgs/msg/right_foot_led.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_action/rclcpp_action.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "std_msgs/msg/color_rgba.hpp"

namespace nao_led_action_server
{

using namespace std::chrono_literals;

using LedsPlay = nao_led_interfaces::action::LedsPlay;
using GoalHandleLedsPlay = rclcpp_action::ServerGoalHandle<LedsPlay>;
using LedIndexes = nao_led_interfaces::msg::LedIndexes;
using LedModes = nao_led_interfaces::msg::LedModes;

LedsPlayActionServer::LedsPlayActionServer(const rclcpp::NodeOptions & options)
: rclcpp::Node("leds_play_action_server_node", options)
{
  using std::placeholders::_1;
  using std::placeholders::_2;

  action_server_ = rclcpp_action::create_server<LedsPlay>(
    this,
    "leds_play",
    std::bind(&LedsPlayActionServer::handleGoal, this, _1, _2),
    std::bind(&LedsPlayActionServer::handleCancel, this, _1),
    std::bind(&LedsPlayActionServer::handleAccepted, this, _1));

  head_pub_ = create_publisher<nao_lola_command_msgs::msg::HeadLeds>("effectors/head_leds", 10);
  right_eye_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightEyeLeds>("effectors/right_eye_leds", 10);
  left_eye_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftEyeLeds>("effectors/left_eye_leds", 10);
  right_ear_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightEarLeds>("effectors/right_ear_leds", 10);
  left_ear_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftEarLeds>("effectors/left_ear_leds", 10);
  chest_pub_ = create_publisher<nao_lola_command_msgs::msg::ChestLed>("effectors/chest_led", 10);
  right_foot_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightFootLed>("effectors/right_foot_led", 10);
  left_foot_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftFootLed>("effectors/left_foot_led", 10);

  color_off_.r = 0.0F;
  color_off_.g = 0.0F;
  color_off_.b = 0.0F;
  color_off_.a = 1.0F;

  RCLCPP_INFO(get_logger(), "LedsPlayActionServer Initialized");
}

LedsPlayActionServer::~LedsPlayActionServer()
{
  stop_requested_.store(true);
  {
    std::lock_guard<std::mutex> lock(worker_mutex_);
    if (worker_.joinable()) {
      worker_.join();
    }
  }
}

rclcpp_action::GoalResponse LedsPlayActionServer::handleGoal(
  const rclcpp_action::GoalUUID & uuid,
  std::shared_ptr<const LedsPlay::Goal> goal)
{
  (void)uuid;
  (void)goal;
  RCLCPP_INFO(get_logger(), "Received LED goal request");
  return rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
}

rclcpp_action::CancelResponse LedsPlayActionServer::handleCancel(
  const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  (void)goal_handle;
  RCLCPP_INFO(get_logger(), "Received request to cancel LED goal");
  return rclcpp_action::CancelResponse::ACCEPT;
}

void LedsPlayActionServer::handleAccepted(const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  std::shared_ptr<GoalHandleLedsPlay> previous_goal;
  {
    std::lock_guard<std::mutex> g(goal_mutex_);
    previous_goal = active_goal_.lock();
  }

  stop_requested_.store(true);
  {
    std::lock_guard<std::mutex> lock(worker_mutex_);
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  if (previous_goal && previous_goal->is_active()) {
    auto res = std::make_shared<LedsPlay::Result>();
    res->success = false;
    previous_goal->abort(res);
    RCLCPP_INFO(get_logger(), "Previous goal aborted due to preemption");
  }

  stop_requested_.store(false);
  {
    std::lock_guard<std::mutex> g(goal_mutex_);
    active_goal_ = goal_handle;
  }

  std::lock_guard<std::mutex> lock(worker_mutex_);
  worker_ = std::thread([this, goal_handle]() { execute(goal_handle); });
}

bool LedsPlayActionServer::shouldStop(const std::shared_ptr<GoalHandleLedsPlay> & goal_handle) const
{
  if (!rclcpp::ok()) {
    return true;
  }
  if (stop_requested_.load()) {
    return true;
  }
  if (goal_handle && goal_handle->is_canceling()) {
    return true;
  }
  return false;
}

void LedsPlayActionServer::execute(const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  RCLCPP_INFO(get_logger(), "Executing LED goal");

  const auto goal = goal_handle->get_goal();

  const std::array<uint8_t, 2> leds = goal->leds;
  const uint8_t mode = goal->mode;
  const float frequency = goal->frequency;
  const std::array<std_msgs::msg::ColorRGBA, 8> colors = goal->colors;
  const std::array<float, 12> intensities = goal->intensities;
  const float duration = goal->duration;

  auto result = std::make_shared<LedsPlay::Result>();
  bool stopped_early = false;

  if (mode == LedModes::STEADY) {
    stopped_early = steadyMode(goal_handle, leds, colors, intensities, duration);
  } else if (mode == LedModes::BLINKING) {
    stopped_early = blinkingMode(goal_handle, leds, frequency, colors, duration);
  } else if (mode == LedModes::LOOP) {
    stopped_early = loopMode(goal_handle, leds, frequency, colors, duration);
  } else {
    publishOff(leds);
    result->success = false;
    if (goal_handle->is_active()) {
      goal_handle->abort(result);
    }
    return;
  }

  if (stopped_early) {
    if (goal_handle->is_canceling()) {
      result->success = true;
      goal_handle->canceled(result);
      RCLCPP_INFO(get_logger(), "LED Goal canceled");
      return;
    }

    if (goal_handle->is_active()) {
      result->success = false;
      goal_handle->abort(result);
      RCLCPP_INFO(get_logger(), "LED Goal aborted (preempt/stop)");
    }
    return;
  }

  if (rclcpp::ok()) {
    result->success = true;
    goal_handle->succeed(result);
    RCLCPP_INFO(get_logger(), "LED Goal succeeded");
  }
}

void LedsPlayActionServer::publishOff(const std::array<uint8_t, 2> & leds)
{
  if (leds[0] == LedIndexes::HEAD) {
    nao_lola_command_msgs::msg::HeadLeds msg;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
      msg.intensities[i] = 0.0F;
    }
    head_pub_->publish(msg);
  }

  if (
    (leds[0] == LedIndexes::REYE && leds[1] == LedIndexes::LEYE) ||
    (leds[0] == LedIndexes::LEYE && leds[1] == LedIndexes::REYE))
  {
    nao_lola_command_msgs::msg::RightEyeLeds r;
    nao_lola_command_msgs::msg::LeftEyeLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
      r.colors[i] = color_off_;
      l.colors[i] = color_off_;
    }
    right_eye_pub_->publish(r);
    left_eye_pub_->publish(l);
  }

  if (
    (leds[0] == LedIndexes::REAR && leds[1] == LedIndexes::LEAR) ||
    (leds[0] == LedIndexes::LEAR && leds[1] == LedIndexes::REAR))
  {
    nao_lola_command_msgs::msg::RightEarLeds r;
    nao_lola_command_msgs::msg::LeftEarLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
      r.intensities[i] = 0.0F;
      l.intensities[i] = 0.0F;
    }
    right_ear_pub_->publish(r);
    left_ear_pub_->publish(l);
  }

  if (leds[0] == LedIndexes::CHEST) {
    nao_lola_command_msgs::msg::ChestLed msg;
    msg.color = color_off_;
    chest_pub_->publish(msg);
  }
}

bool LedsPlayActionServer::steadyMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  const std::array<float, 12> & intensities,
  float duration_s)
{
  auto has_led = [&](uint8_t idx) -> bool { return leds[0] == idx || leds[1] == idx; };

  auto normalize_alpha = [](std_msgs::msg::ColorRGBA & c) {
    if (c.a == 0.0F) c.a = 1.0F;
  };

  auto is_zero_rgb = [](const std_msgs::msg::ColorRGBA & c) -> bool {
    return (c.r == 0.0F && c.g == 0.0F && c.b == 0.0F);
  };

  std::array<std_msgs::msg::ColorRGBA, 8> eye_colors = colors_in;
  for (auto & c : eye_colors) normalize_alpha(c);

  bool any_nonzero_after0 = false;
  for (int i = 1; i < 8; ++i) {
    if (!is_zero_rgb(eye_colors[i])) {
      any_nonzero_after0 = true;
      break;
    }
  }
  if (!any_nonzero_after0 && !is_zero_rgb(eye_colors[0])) {
    for (int i = 1; i < 8; ++i) eye_colors[i] = eye_colors[0];
  }

  auto publish_off_subset = [&]() {
    // HEAD
    if (has_led(LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = 0.0F;
      }
      head_pub_->publish(msg);
    }

    // RIGHT EYE
    if (has_led(LedIndexes::REYE)) {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
        r.colors[i] = color_off_;
      }
      right_eye_pub_->publish(r);
    }

    // LEFT EYE
    if (has_led(LedIndexes::LEYE)) {
      nao_lola_command_msgs::msg::LeftEyeLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
        l.colors[i] = color_off_;
      }
      left_eye_pub_->publish(l);
    }

    // RIGHT EAR
    if (has_led(LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = 0.0F;
      }
      right_ear_pub_->publish(r);
    }

    // LEFT EAR
    if (has_led(LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
        l.intensities[i] = 0.0F;
      }
      left_ear_pub_->publish(l);
    }

    // CHEST
    if (has_led(LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = color_off_;
      chest_pub_->publish(msg);
    }
  };

  if (has_led(LedIndexes::HEAD)) {
    nao_lola_command_msgs::msg::HeadLeds msg;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
      msg.intensities[i] = intensities[i];
    }
    head_pub_->publish(msg);
  }

  if (has_led(LedIndexes::REYE)) {
    nao_lola_command_msgs::msg::RightEyeLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
      r.colors[i] = eye_colors[i];
    }
    right_eye_pub_->publish(r);
  }

  if (has_led(LedIndexes::LEYE)) {
    nao_lola_command_msgs::msg::LeftEyeLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
      l.colors[i] = eye_colors[i];
    }
    left_eye_pub_->publish(l);
  }

  if (has_led(LedIndexes::REAR)) {
    nao_lola_command_msgs::msg::RightEarLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
      r.intensities[i] = intensities[i];
    }
    right_ear_pub_->publish(r);
  }

  if (has_led(LedIndexes::LEAR)) {
    nao_lola_command_msgs::msg::LeftEarLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
      l.intensities[i] = intensities[i];
    }
    left_ear_pub_->publish(l);
  }

  if (has_led(LedIndexes::CHEST)) {
    nao_lola_command_msgs::msg::ChestLed msg;
    msg.color = eye_colors[0];
    chest_pub_->publish(msg);
  }
  if (duration_s < 0.0F) {
    rclcpp::Rate rate(50.0);
    while (!shouldStop(goal_handle)) {
      rate.sleep();
    }
    publish_off_subset();
    return true;
  }

  if (duration_s == 0.0F) {
    publish_off_subset();
    return false;
  }

  const rclcpp::Time start = now();
  rclcpp::Rate rate(50.0);
  while (!shouldStop(goal_handle)) {
    if ((now() - start).seconds() >= duration_s) break;
    rate.sleep();
  }

  if (shouldStop(goal_handle)) {
    publish_off_subset();
    return true;
  }

  publish_off_subset();
  return false;
}

bool LedsPlayActionServer::blinkingMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  float frequency_hz,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  float duration_s)
{
  if (frequency_hz <= 0.0F) {
    publishOff(leds);
    return true;
  }

  auto has_led = [&leds](uint8_t idx) -> bool {
    return (leds[0] == idx) || (leds[1] == idx);
  };

  auto is_zero_rgb = [](const std_msgs::msg::ColorRGBA & c) -> bool {
    return (c.r == 0.0F && c.g == 0.0F && c.b == 0.0F);
  };

  auto normalize_alpha = [](std_msgs::msg::ColorRGBA & c) {
    if (c.a == 0.0F) {
      c.a = 1.0F;
    }
  };

  std::array<std_msgs::msg::ColorRGBA, 8> eye_colors = colors_in;
  for (auto & c : eye_colors) {
    normalize_alpha(c);
  }

  bool any_nonzero_after0 = false;
  for (int i = 1; i < 8; ++i) {
    if (!is_zero_rgb(eye_colors[i])) {
      any_nonzero_after0 = true;
      break;
    }
  }
  if (!any_nonzero_after0 && !is_zero_rgb(eye_colors[0])) {
    for (int i = 1; i < 8; ++i) {
      eye_colors[i] = eye_colors[0];
      normalize_alpha(eye_colors[i]);
    }
  }

  const rclcpp::Time start = now();
  rclcpp::Rate loop_rate(frequency_hz);

  bool on = false;

  while (!shouldStop(goal_handle)) {
    if (duration_s >= 0.0F && (now() - start).seconds() >= duration_s) {
      break;
    }

    on = !on;

    if (has_led(LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = on ? 1.0F : 0.0F;
      }
      head_pub_->publish(msg);
    }

    if (has_led(LedIndexes::REYE)) {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
        r.colors[i] = on ? eye_colors[i] : color_off_;
      }
      right_eye_pub_->publish(r);
    }

    if (has_led(LedIndexes::LEYE)) {
      nao_lola_command_msgs::msg::LeftEyeLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
        l.colors[i] = on ? eye_colors[i] : color_off_;
      }
      left_eye_pub_->publish(l);
    }

    if (has_led(LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = on ? 1.0F : 0.0F;
      }
      right_ear_pub_->publish(r);
    }

    if (has_led(LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
        l.intensities[i] = on ? 1.0F : 0.0F;
      }
      left_ear_pub_->publish(l);
    }

    if (has_led(LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = on ? eye_colors[0] : color_off_;
      chest_pub_->publish(msg);
    }

    loop_rate.sleep();
  }

  publishOff(leds);
  return shouldStop(goal_handle);
}

bool LedsPlayActionServer::loopMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  float frequency_hz,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors,
  float duration_s)
{
  if (frequency_hz <= 0.0F) {
    publishOff(leds);
    return true;
  }

  const rclcpp::Time start = now();
  rclcpp::Rate loop_rate(frequency_hz);

  uint8_t c = 0;

  while (!shouldStop(goal_handle)) {
    if (duration_s >= 0.0F && (now() - start).seconds() >= duration_s) {
      break;
    }

    if (leds[0] == LedIndexes::HEAD) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = 1.0F;
      }
      c = static_cast<uint8_t>((c + 1U) % nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS);
      msg.intensities[c] = 0.0F;
      head_pub_->publish(msg);
    }

    if (
      (leds[0] == LedIndexes::REYE && leds[1] == LedIndexes::LEYE) ||
      (leds[0] == LedIndexes::LEYE && leds[1] == LedIndexes::REYE))
    {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      nao_lola_command_msgs::msg::LeftEyeLeds l;

      constexpr short int n = nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS;
      static short int step = 0;

      for (short int i = 0; i < n; ++i) {
        r.colors[i] = colors[i];
        l.colors[i] = colors[i];
      }

      step = static_cast<short int>((step + 1) % n);
      const short int cw = step;
      const short int cw_succ = static_cast<short int>((cw + 1) % n);
      const short int ccw = (cw != 0) ? static_cast<short int>(n - cw) : 0;
      const short int ccw_succ = (ccw != 0) ? static_cast<short int>(ccw - 1) : static_cast<short int>(n - 1);

      r.colors[cw] = color_off_;
      r.colors[cw_succ] = color_off_;
      l.colors[ccw] = color_off_;
      l.colors[ccw_succ] = color_off_;

      right_eye_pub_->publish(r);
      left_eye_pub_->publish(l);
    }

    if (
      (leds[0] == LedIndexes::REAR && leds[1] == LedIndexes::LEAR) ||
      (leds[0] == LedIndexes::LEAR && leds[1] == LedIndexes::REAR))
    {
      nao_lola_command_msgs::msg::RightEarLeds r;
      nao_lola_command_msgs::msg::LeftEarLeds l;

      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = 1.0F;
        l.intensities[i] = 1.0F;
      }

      c = static_cast<uint8_t>((c + 1U) % nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS);
      r.intensities[c] = 0.0F;
      l.intensities[c] = 0.0F;

      right_ear_pub_->publish(r);
      left_ear_pub_->publish(l);
    }

    loop_rate.sleep();
  }

  publishOff(leds);
  return shouldStop(goal_handle);
}

}  // namespace nao_led_action_server

RCLCPP_COMPONENTS_REGISTER_NODE(nao_led_action_server::LedsPlayActionServer)
